import os
from typing import Callable, List, NotRequired, TypedDict

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table, UpdateSchema
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from servc.com.service import ComponentType, ServiceComponent


def flatten(xss):
    return [x for xs in xss for x in xs]


class CatalogueConfig(TypedDict):
    name: str
    namespace: str
    uri: str
    io_impl: NotRequired[str]
    type: NotRequired[str]


class Migration(TypedDict):
    version: float
    migrate: Callable[[UpdateSchema, Table], bool]


class IcebergTable(TypedDict):
    tablename: str
    migrations: List[Migration]
    schema: Schema
    partition: NotRequired[PartitionSpec]
    sort_order: NotRequired[SortOrder]
    properties: NotRequired[dict]


class Iceberg(ServiceComponent):
    _type: ComponentType = ComponentType.DATABASE

    _config: CatalogueConfig = None

    _catalog: Catalog = None

    _tables: List[IcebergTable] = []

    _dataPath: str = None

    def __init__(
        self, datapath: str, config: CatalogueConfig, tables: List[IcebergTable] = []
    ):
        super().__init__()

        self._config = {
            **config,
            "type": config.get("type", "sql"),
            "py-io-impl": config.get("io_impl", "pyiceberg.io.pyarrow.PyArrowFileIO"),
        }
        self._tables = tables
        self._dataPath = datapath

    def _connect(self):
        self._catalog = load_catalog(**self._config)

        try:
            namespaces = flatten(self._catalog.list_namespaces())
            if self._config["namespace"] not in namespaces:
                self._catalog.create_namespace(self._config["namespace"])
        except Exception as e:
            print(e)
            print("Failed to list namespaces, initializing catalog")
            self._catalog.create_tables()
            return self._connect()
        self.load_tables()

        self._isReady = True
        self._isOpen = True

    def _close(self):
        self._isOpen = False
        self._isReady = False

    def get_table(self, name: str) -> Table:
        return self._catalog.load_table((self._config["namespace"], name))

    def load_tables(self):
        catalog = self._catalog
        config = self._config
        tableConfigs = self._tables

        existingTables = flatten(catalog.list_tables(config["namespace"]))
        for tableConfig in tableConfigs:
            identifier = tuple([config["namespace"], tableConfig["tablename"]])
            location = os.path.join(self._dataPath, *identifier)

            # make sure folders exist
            for folder in ["data", "metadata"]:
                if not os.path.exists(os.path.join(location, folder)):
                    os.makedirs(os.path.join(location, folder), exist_ok=True)

            if tableConfig["tablename"] not in existingTables:
                catalog.create_table(
                    identifier=identifier,
                    schema=tableConfig["schema"],
                    location=location,
                    partition_spec=tableConfig.get(
                        "partition", UNPARTITIONED_PARTITION_SPEC
                    ),
                    sort_order=tableConfig.get("sort_order", UNSORTED_SORT_ORDER),
                    properties={
                        **tableConfig.get("properties", {}),
                        "migrationVersion": "0",
                    },
                )
            table = catalog.load_table(identifier)

            # check if the migration needs to be run
            for migration in tableConfig["migrations"]:
                currentVersion = int(table.properties["migrationVersion"])
                if migration["version"] > currentVersion:
                    with table.update_schema() as schema:
                        migration["migrate"](schema, table)

                    with table.transaction() as transaction:
                        transaction.set_properties(
                            migrationVersion=str(migration["version"])
                        )

    def add_table(self, table: IcebergTable, load=True) -> bool:
        self._tables.append(table)
        if load:
            self.load_tables()
            return True
        return False

    def delete_table(self, tableName: str) -> bool:
        for index, table in enumerate(self._tables):
            if table["tablename"] == tableName:
                del self._tables[index]
                self._catalog.drop_table((self._config["namespace"], tableName))
                return True
        return False

    @property
    def catalog(self) -> Catalog:
        return self._catalog

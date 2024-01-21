import os
import shutil
import unittest

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

from servciceberg import Iceberg
from servciceberg.config import CATALOGUE_NAME, CATALOGUE_NAMESPACE, CATALOGUE_URI

df = pa.Table.from_pylist(
    [
        {"col": "df1", "value": 1},
        {"col": "asd2", "value": 2},
        {"col": "asd3", "value": 3},
        {"col": "asd4", "value": 4},
        {"col": "asd5", "value": 5},
    ],
    schema=pa.schema([pa.field("col", pa.string()), pa.field("value", pa.int32())]),
)

table = {
    "tablename": "mytesttable",
    "migrations": [],
    "schema": Schema(
        NestedField(field_id=1, name="col", field_type=StringType(), required=False),
        NestedField(field_id=2, name="value", field_type=IntegerType(), required=False),
    ),
}


class TestServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.iceberg = Iceberg(
            "/tmp/iceberg",
            {
                "name": CATALOGUE_NAME,
                "namespace": CATALOGUE_NAMESPACE,
                "uri": CATALOGUE_URI,
            },
        )
        cls.iceberg.connect()

    @classmethod
    def tearDownClass(cls):
        cls.iceberg.delete_table(table["tablename"])
        shutil.rmtree("/tmp/iceberg")

    def test_create_table(self):
        self.iceberg.add_table(table)

        metadata = os.listdir(
            os.path.join(
                "/tmp/iceberg", CATALOGUE_NAMESPACE, table["tablename"], "metadata"
            )
        )
        self.assertGreater(len(metadata), 0)

    def test_write_table(self):
        tbl = self.iceberg.get_table(table["tablename"])
        tbl.append(df)

        values = tbl.scan().to_pandas()
        self.assertEqual(len(values), 5)

    # def test_migrations(self):
    #     table["migrations"] = [
    #         {
    #             "version": 1,
    #             "migrate": lambda schema, table: schema.update_column(
    #                 "value", required=False
    #             ),
    #         }
    #     ]
    #     self.iceberg.load_tables()


if __name__ == "__main__":
    unittest.main()

import os

DATA_PATH = os.getenv("DATA_PATH", "/tmp")
CATALOGUE_NAME = os.getenv("CATALOGUE_NAME", "default")
CATALOGUE_NAMESPACE = os.getenv("CATALOGUE_NAMESPACE", "namespace")
CATALOGUE_TYPE = os.getenv("CATALOGUE_TYPE", "sql")
CATALOGUE_URI = os.getenv(
    "CATALOGUE_URI",
    os.getenv(
        "DB_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres"
    ),
)

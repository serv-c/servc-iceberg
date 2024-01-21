#!/usr/bin/env bash
set -x

rm -rf iceberg || true
git clone https://github.com/apache/iceberg-python iceberg
cd iceberg
pip install poetry
python3 -m poetry build --format=sdist
python -m pip install cibuildwheel==2.16.2

export CIBW_ARCHS="auto64"
export CIBW_TEST_SKIP="pp* *macosx*"
export CIBW_TEST_EXTRAS="s3fs,glue"
export CIBW_TEST_COMMAND="pytest {project}/tests/avro/test_decoder.py"
make install
pip install dist/pyiceberg-0.6.0.tar.gz
cd ..
rm -rf iceberg
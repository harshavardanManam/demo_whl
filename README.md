# Protobuf ingestion
## Entrypoint
`envelope_generator`

## Building python wheel
Under root directory run `python setup.py bdist_wheel`
Wheel file located in directory `dist/`

## Prerequisite
`spark-protobuf_2.12-3.4.0-SNAPSHOT.jar` need to be installed as cluster library

`proto_ingestion-<version>-py3-none-any.whl` need to be installed as cluster library

`dbldatagen==0.3.0` need to be installed as cluster library
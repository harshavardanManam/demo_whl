import dbldatagen as dg
import pyspark.sql.types as T

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path

# datatype_id = 'b7e11d31-cdcd-4842-beb0-c2d7c897b9da'

# schema
# root
#  |-- BiosVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ChassisType: string (nullable = true)
#  |-- ChassisCatalogue: string (nullable = true)
#  |-- Name: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ServiceTag: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- AssetTag: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ManufactureDate: timestamp (nullable = true)
#  |-- FirstPowerDate: timestamp (nullable = true)
#  |-- Hostname: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Capability: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- value: string (nullable = true)
#  |-- Manufacturer: struct (nullable = true)
#  |    |-- value: string (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "BiosVersion",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {"name": "ChassisType", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "ChassisCatalogue",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Name",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "ServiceTag",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "AssetTag",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "ManufactureDate",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FirstPowerDate",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Hostname",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Capability",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "value",
                            "type": "string",
                            "nullable": True,
                            "metadata": {},
                        }
                    ],
                },
                "containsNull": False,
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Manufacturer",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
    ],
}

current_schema = {
    "type": "struct",
    "fields": [
        {"name": "BiosVersion", "type": "string", "nullable": True, "metadata": {}},
        {"name": "ChassisType", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "ChassisCatalogue",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "ServiceTag", "type": "string", "nullable": True, "metadata": {}},
        {"name": "AssetTag", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "ManufactureDate",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FirstPowerDate",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Hostname", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "Capability",
            "type": {"type": "array", "elementType": "string", "containsNull": True},
            "nullable": True,
            "metadata": {},
        },
        {"name": "Manufacturer", "type": "string", "nullable": True, "metadata": {}},
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('SystemInformation', 'SystemInformation')

  spark = ( 
    SparkSession
    .builder
    .appName('protobuf generator')
    .getOrCreate()
  )

  dataspec = (
    dg.DataGenerator(
      spark, 
      rows=rows, 
      partitions=partitions, 
      randomSeedMethod='hash_fieldname'
    )
    .withSchema(table_schema)
  )

  #########################################

  dataspec = (
    dataspec
    .withIdOutput()
    .withColumnSpec(
      'BiosVersion',
      template=r'1.dd.d'
    )
    .withColumnSpec(
      'ChassisType',
      values=['Notebook']
    )
    .withColumnSpec(
      'ChassisCatalogue',
      values=['LaptopCatalogue']
    )
    .withColumnSpec(
      'Name',
      values=['Latitude 5420', 'Precision 7550', 'Precision 7560']
    )
    .withColumnSpec(
      'ServiceTag',
      values=['JZ2H3J3', '4GLB353', 'C6PGBK3']
    )
    .withColumnSpec(
      'AssetTag',
      values=[None]
    )
    .withColumnSpec(
      'ManufactureDate',
      begin='2022-12-07 00:00:00', 
      end='2023-02-07 23:59:00', 
      interval='1 second'
    )
    .withColumnSpec(
      'FirstPowerDate',
      baseColumn='ManufactureDate',
      expr='add_months(ManufactureDate, 3)'
    )
    .withColumnSpec(
      'Hostname',
      values=['W10JZ2H3J3', 'W114GLB353', 'W10C6PGBK3']
    )
    .withColumnSpec(
      'Capability',
      values=[None]
    )
    .withColumnSpec(
      'Manufacturer',
      values=['Dell Inc.']
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )

  # Nested fields need to be added:
  #  |-- Capability: array (nullable = true)
  #  |    |-- element: struct (containsNull = false)
  #  |    |    |-- value: string (nullable = true)
  add_nest_field_df = (
    proto_df
    .withColumn(
      'Capability', 
      F.array(
        F.struct(
          F.lit('SystemInfo').alias('value')
        )
      )
    )
  )

  proto_original_df = set_nest_col(add_nest_field_df, original_schema)
  proto_col_df = df_to_col(proto_original_df, proto_original_df.columns[1:], 'proto_message')

  raw_proto_df = ( 
    proto_col_df
    .withColumn(
      'serialized_blob', 
      to_protobuf(
        F.col('proto_message'), 
        message_name, 
        get_desc_path(f'{proto_file}.desc')
      )
    )
    .select('id', 'serialized_blob')
  )

  return raw_proto_df
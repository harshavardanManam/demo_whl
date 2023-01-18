from array import ArrayType
import dbldatagen as dg
import pyspark.sql.types as T
import random

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path


random.seed(42)

# datatype_id = '70350172-f987-4e0b-9b9b-3192d8fdf17d'

# schema
# root
#  |-- BootMode: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- IsUEFISecureBoot: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- Manufacturer: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Name: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Version: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- SerialNumber: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- IsHostingBoard: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsHotSwappable: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsPoweredOn: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsRemovable: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsReplaceable: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsRequiresDaughterBoard: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- FeatureFlags: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- value: string (nullable = true)
#  |-- BaseboardEPPID: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- BaseboardPPID: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Capability: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- value: string (nullable = true)
#  |-- Description: struct (nullable = true)
#  |    |-- value: string (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "BootMode",
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
            "name": "IsUEFISecureBoot",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
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
            "name": "Version",
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
            "name": "SerialNumber",
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
            "name": "IsHostingBoard",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsHotSwappable",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsPoweredOn",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsRemovable",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsReplaceable",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsRequiresDaughterBoard",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FeatureFlags",
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
            "name": "BaseboardEPPID",
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
            "name": "BaseboardPPID",
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
            "name": "Description",
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
        {"name": "BootMode", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "IsUEFISecureBoot",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Manufacturer", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Version", "type": "string", "nullable": True, "metadata": {}},
        {"name": "SerialNumber", "type": "string", "nullable": True, "metadata": {}},
        {"name": "IsHostingBoard", "type": "boolean", "nullable": True, "metadata": {}},
        {"name": "IsHotSwappable", "type": "boolean", "nullable": True, "metadata": {}},
        {"name": "IsPoweredOn", "type": "boolean", "nullable": True, "metadata": {}},
        {"name": "IsRemovable", "type": "boolean", "nullable": True, "metadata": {}},
        {"name": "IsReplaceable", "type": "boolean", "nullable": True, "metadata": {}},
        {
            "name": "IsRequiresDaughterBoard",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FeatureFlags",
            "type": {"type": "array", "elementType": "string", "containsNull": False},
            "nullable": True,
            "metadata": {},
        },
        {"name": "BaseboardEPPID", "type": "string", "nullable": True, "metadata": {}},
        {"name": "BaseboardPPID", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "Capability",
            "type": {"type": "array", "elementType": "string", "containsNull": False},
            "nullable": True,
            "metadata": {},
        },
        {"name": "Description", "type": "string", "nullable": True, "metadata": {}},
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('SystemboardInfo', 'SystemboardInfo')

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
      'BootMode',
      values=['UEFI']
    )
    .withColumnSpec(
      'IsUEFISecureBoot',
      values=[True]
    )
    .withColumnSpec(
      'Manufacturer',
      values=['Dell Inc.']
    )
    .withColumnSpec(
      'Name',
      values=['01PXFR', '01C06K', '01M3M4']
    )
    .withColumnSpec(
      'Version',
      values=['A00', 'A01', 'A00']
    )
    .withColumnSpec(
      'SerialNumber',
      values=['/4GLB353/CNCMK00076000B/', '/C6PGBK3/CNCMK001C9013A/', '/JZ2H3J3/CNCMA0019Q008E/']
    )
    .withColumnSpec(
      'IsHostingBoard',
      values=[True]
    )
    .withColumnSpec(
      'IsHotSwappable',
      values=[None]
    )
    .withColumnSpec(
      'IsPoweredOn',
      values=[True]
    )
    .withColumnSpec(
      'IsRemovable',
      values=[None]
    )
    .withColumnSpec(
      'IsReplaceable',
      values=[True]
    )
    .withColumnSpec(
      'IsRequiresDaughterBoard',
      values=[None]
    )
    .withColumnSpec(
      'FeatureFlags',
      values=[None]
    )
    .withColumnSpec(
      'BaseboardEPPID',
      values=['CN01PXFRCMK00076000BA00', 'CN01C06KCMK001C9013AA01', 'CN01M3M4CMA0019Q008EA00']
    )
    .withColumnSpec(
      'BaseboardPPID',
      values=['CNCMK00076000B', 'CNCMK001C9013A', 'CNCMA0019Q008E']
    )
    .withColumnSpec(
      'Capability',
      values=[None]
    )
    .withColumnSpec(
      'Description',
      values=['Systemboard']
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )

  # Nested fields need to be added:
  # FeatureFlags is array
  # FeatureFlags: array (nullable = true)
  #  |-- element: struct (containsNull = false)
  #  |    |-- value: string (nullable = true)

  # Capability is array
  # Capability: array (nullable = true)
  #  |-- element: struct (containsNull = false)
  #  |    |-- value: string (nullable = true)

  add_nest_field_df = (
    proto_df
    .withColumn(
      'FeatureFlags', 
      F.array(
        F.struct(F.lit('HostingBoard').alias('value')),
        F.struct(F.lit('PoweredOn').alias('value')),
        F.struct(F.lit('Replaceable').alias('value')),
      )
    )
    .withColumn(
      'Capability', 
      F.array(
        F.struct(F.lit('SystemBoard').alias('value')),
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
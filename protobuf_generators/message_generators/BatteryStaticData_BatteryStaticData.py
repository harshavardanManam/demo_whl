import dbldatagen as dg
import pyspark.sql.types as T

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path

# datatype_id = 'b9bdc4e0-3d19-4cd0-941c-b51332d4a87f'

# schema
# root
#  |-- LocationInText: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Location: string (nullable = true)
#  |-- Capability: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- value: string (nullable = true)
#  |-- FullChargeCapacityInmAh: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- FullChargeCapacityInmWh: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- DesignCapacityInmAh: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- DesignCapacityInmWh: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- DesignVoltageInmV: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- SerialNumber: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- BattmanCapability: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- ManufactureDate: timestamp (nullable = true)
#  |-- SpecificationInfo: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- Version: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- Revision: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- Manufacturer: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Name: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Chemistry: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Eppid: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Description: struct (nullable = true)
#  |    |-- value: string (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "LocationInText",
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
        {"name": "Location", "type": "string", "nullable": True, "metadata": {}},
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
            "name": "FullChargeCapacityInmAh",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FullChargeCapacityInmWh",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DesignCapacityInmAh",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DesignCapacityInmWh",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DesignVoltageInmV",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "integer",
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
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "BattmanCapability",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "integer",
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
            "name": "SpecificationInfo",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "integer",
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
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Revision",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "value",
                        "type": "integer",
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
            "name": "Chemistry",
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
            "name": "Eppid",
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
        {"name": "LocationInText", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Location", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "Capability",
            "type": {"type": "array", "elementType": "string", "containsNull": False},
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FullChargeCapacityInmAh",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FullChargeCapacityInmWh",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DesignCapacityInmAh",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DesignCapacityInmWh",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DesignVoltageInmV",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {"name": "SerialNumber", "type": "integer", "nullable": True, "metadata": {}},
        {
            "name": "BattmanCapability",
            "type": "integer",
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
            "name": "SpecificationInfo",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Version", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "Revision", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "Manufacturer", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Chemistry", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Eppid", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Description", "type": "string", "nullable": True, "metadata": {}},
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('BatteryStaticData', 'BatteryStaticData')

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
      'LocationInText',
      values=['LeftBayBattery']
    )
    .withColumnSpec(
      'Location',
      values=['LeftBayBattery']
    )
    .withColumnSpec(
      'Capability',
      values=[None]
    )
    .withColumnSpec(
      'FullChargeCapacityInmAh',
      baseColumn='DesignCapacityInmAh',
      expr='''
      (
        case
          when DesignCapacityInmAh = 4145 then (1 - rand(0) * 0.5) * 4145
          when DesignCapacityInmAh = 8334 then (1 - rand(0) * 0.5) * 8334
        end
      )
      '''
    )
    .withColumnSpec(
      'FullChargeCapacityInmWh',
      baseColumn='DesignCapacityInmAh',
      expr='''
      (
        case
          when DesignCapacityInmAh = 4145 then (1 - rand(0) * 0.5) * 63004
          when DesignCapacityInmAh = 8334 then (1 - rand(0) * 0.5) * 95007
        end
      )
      '''
    )
    .withColumnSpec(
      'DesignCapacityInmAh',
      baseColumn='SerialNumber',
      expr='''
      (
        case
          when SerialNumber = 1887 then 4145
          when SerialNumber = 477 or SerialNumber = 1007 then 8334
        end
      )
      '''
    )
    .withColumnSpec(
      'DesignCapacityInmWh',
      values=[63004, 95007]
    )
    .withColumnSpec(
      'DesignVoltageInmV',
      values=[15200, 11400]
    )
    .withColumnSpec(
      'SerialNumber',
      values=[1887, 477, 1007]
    )
    .withColumnSpec(
      'BattmanCapability',
      values=[1]
    )
    .withColumnSpec(
      'ManufactureDate',
      begin='2022-12-07 00:00:00', 
      end='2023-02-07 23:59:00', 
      interval='1 minute'    
    )
    .withColumnSpec(
      'SpecificationInfo',
      values=[49]
    )
    .withColumnSpec(
      'Version',
      values=[1]
    )
    .withColumnSpec(
      'Revision',
      values=[1]
    )
    .withColumnSpec(
      'Manufacturer',
      baseColumn='SerialNumber',
      expr='''
      (
        case
          when SerialNumber = 1887 then 'BYD'
          when SerialNumber = 477 or SerialNumber = 1007 then 'SMP'
        end
      )
      '''
    )
    .withColumnSpec(
      'Name',
      values=['DELL M033W18', 'DELL 68ND306', 'DELL 68ND31B']
    )
    .withColumnSpec(
      'Chemistry',
      values=['LiP']
    )
    .withColumnSpec(
      'Eppid',
      values=['CN0M033WBDS0018257INA01', 'CN068ND3SLW0006G80SEA00', 'CN068ND3SLW001BJ40RZA03']
    )
    .withColumnSpec(
      'Description',
      values=['Battery']
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )

  # Nested fields need to be added:
  # Capability is array
  #  |-- Capability: array (nullable = true)
  #  |    |-- element: struct (containsNull = false)
  #  |    |    |-- value: string (nullable = true)
  add_nest_field_df = (
    proto_df
    .withColumn(
      'Capability', 
      F.array(
        F.struct(F.lit('Battery').alias('value')),
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
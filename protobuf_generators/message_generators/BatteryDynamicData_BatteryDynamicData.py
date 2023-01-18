from array import ArrayType
import dbldatagen as dg
import pyspark.sql.types as T
import random

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path


random.seed(42)

# datatype_id = '9f45e5f6-4599-4494-a0f4-8a710d29ac59'

# schema
# root
#  |-- TemperatureInKelvin: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- VoltageInmV: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- CurrentInmA: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- CurrentChargePercentage: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- RemainingCapacityInmAh: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- RemainingCapacityInmWh: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- CycleCount: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- ChargeStatus: string (nullable = true)
#  |-- Status: string (nullable = true)
#  |-- ManufacturerAccessCode: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- FullChargeCapacityInmAh: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- FullChargeCapacityInmWh: struct (nullable = true)
#  |    |-- value: integer (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "TemperatureInKelvin",
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
            "name": "VoltageInmV",
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
            "name": "CurrentInmA",
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
            "name": "CurrentChargePercentage",
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
            "name": "RemainingCapacityInmAh",
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
            "name": "RemainingCapacityInmWh",
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
            "name": "CycleCount",
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
        {"name": "ChargeStatus", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Status", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "ManufacturerAccessCode",
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
    ],
}

current_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "TemperatureInKelvin",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {"name": "VoltageInmV", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "CurrentInmA", "type": "integer", "nullable": True, "metadata": {}},
        {
            "name": "CurrentChargePercentage",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "RemainingCapacityInmAh",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "RemainingCapacityInmWh",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {"name": "CycleCount", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "ChargeStatus", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Status", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "ManufacturerAccessCode",
            "type": "integer",
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
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('BatteryDynamicData', 'BatteryDynamicData')

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
      'TemperatureInKelvin',
      minValue=298,
      maxValue=311,
      percentNulls=0.001
    )
    .withColumnSpec(
      'VoltageInmV',
      minValue=12284,
      maxValue=17072,
      percentNulls=0.001
    )
    .withColumnSpec(
      'CurrentInmA',
      minValue=215,
      maxValue=632,
      percentNulls=0.001
    )
    .withColumnSpec(
      'CurrentChargePercentage',
      minValue=50,
      maxValue=100,
      percentNulls=0.001
    )
    .withColumnSpec(
      'RemainingCapacityInmAh',
      baseColumn=['CurrentChargePercentage', 'FullChargeCapacityInmAh'],
      expr='''cast(CurrentChargePercentage * FullChargeCapacityInmAh / 100 as int)'''
    )
    .withColumnSpec(
      'RemainingCapacityInmWh',
      baseColumn=['CurrentChargePercentage', 'FullChargeCapacityInmWh'],
      expr='''cast(CurrentChargePercentage * FullChargeCapacityInmWh / 100 as int)'''
    )
    .withColumnSpec(
      'CycleCount',
      minValue=3,
      maxValue=270,
      percentNulls=0.001
    )
    .withColumnSpec(
      'ChargeStatus',
      values=['AdapterConnected'],
      percentNulls=0.001
    )
    .withColumnSpec(
      'Status',
      values=['OK'],
      percentNulls=0.001
    )
    .withColumnSpec(
      'ManufacturerAccessCode',
      minValue=392,
      maxValue=18312,
      percentNulls=0.001
    )
    .withColumnSpec(
      'FullChargeCapacityInmAh',
      values=[2974, 3062, 3090, 7398, 8334],
      percentNulls=0.001
    )
    .withColumnSpec(
      'FullChargeCapacityInmWh',
      values=[45204, 46542, 46968, 84337, 95007],
      percentNulls=0.001
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )

  add_nest_field_df = (
    proto_df
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
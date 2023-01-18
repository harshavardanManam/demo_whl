from array import ArrayType
import dbldatagen as dg
import pyspark.sql.types as T

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path

# datatype_id = '10466b48-557f-4966-a697-2247d2f3cb4b'

# schema
# root
#  |-- Processors: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- Name: struct (nullable = true)
#  |    |    |    |-- value: string (nullable = true)
#  |    |    |-- PercentC1Time: struct (nullable = true)
#  |    |    |    |-- value: long (nullable = true)
#  |    |    |-- PercentC2Time: struct (nullable = true)
#  |    |    |    |-- value: long (nullable = true)
#  |    |    |-- PercentC3Time: struct (nullable = true)
#  |    |    |    |-- value: long (nullable = true)
#  |    |    |-- PercentIdleTime: struct (nullable = true)
#  |    |    |    |-- value: long (nullable = true)
#  |    |    |-- PercentProcessorTime: struct (nullable = true)
#  |    |    |    |-- value: long (nullable = true)
#  |-- CurrentClockSpeedMHz: struct (nullable = true)
#  |    |-- value: integer (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "Processors",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
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
                            "name": "PercentC1Time",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "nullable": True,
                                        "metadata": {},
                                    }
                                ],
                            },
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentC2Time",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "nullable": True,
                                        "metadata": {},
                                    }
                                ],
                            },
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentC3Time",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "nullable": True,
                                        "metadata": {},
                                    }
                                ],
                            },
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentIdleTime",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "nullable": True,
                                        "metadata": {},
                                    }
                                ],
                            },
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentProcessorTime",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "nullable": True,
                                        "metadata": {},
                                    }
                                ],
                            },
                            "nullable": True,
                            "metadata": {},
                        },
                    ],
                },
                "containsNull": False,
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "CurrentClockSpeedMHz",
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
            "name": "Processors",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "Name",
                            "type": "string",
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentC1Time",
                            "type": "long",
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentC2Time",
                            "type": "long",
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentC3Time",
                            "type": "long",
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentIdleTime",
                            "type": "long",
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "PercentProcessorTime",
                            "type": "long",
                            "nullable": True,
                            "metadata": {},
                        },
                    ],
                },
                "containsNull": False,
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "CurrentClockSpeedMHz",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('CpuDynamicData', 'CpuDynamicData')

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
    .withColumn(
      'num_processor',
      T.IntegerType(),
      values=[1, 8, 16],
      random=True
    )
    .withColumnSpec(
      'Processors',
      values=[None]
    )
    .withColumnSpec(
      'CurrentClockSpeedMHz',
      baseColumn='num_processor',
      expr='''
      (
        case
          when num_processor = 1 then null
          else rand(0) * 2611
        end
      )
      '''
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )


  # Nested fields need to be added:
  # Processors is array
  #  |-- Processors: array (nullable = true)
  #  |    |-- element: struct (containsNull = false)
  #  |    |    |-- Name: struct (nullable = true)
  #  |    |    |    |-- value: string (nullable = true)
  #  |    |    |-- PercentC1Time: struct (nullable = true)
  #  |    |    |    |-- value: long (nullable = true)
  #  |    |    |-- PercentC2Time: struct (nullable = true)
  #  |    |    |    |-- value: long (nullable = true)
  #  |    |    |-- PercentC3Time: struct (nullable = true)
  #  |    |    |    |-- value: long (nullable = true)
  #  |    |    |-- PercentIdleTime: struct (nullable = true)
  #  |    |    |    |-- value: long (nullable = true)
  #  |    |    |-- PercentProcessorTime: struct (nullable = true)
  #  |    |    |    |-- value: long (nullable = true)

  def generate_processor(num_processor: int) -> list:
    import random

    random.seed(42)
    processor_info = []

    for index in range(num_processor):
      idle = random.randint(0, 100)
      c1 = random.randint(0, idle)
      c2 = random.randint(0, idle - c1)
      c3 = random.randint(0, idle - c1 - c2)
      pt = 100 - idle
      name = f'Processor{index}'
      if idle == 0: idle = c1 = c2 = c3 = None
      if c1 == 0: c1 = None
      if c2 == 0: c2 = None
      if c3 == 0: c3 = None

      if num_processor == 1:
        name = 'Processor'

      processor_info.append(
        pyspark.sql.Row(
          Name = pyspark.sql.Row(value=name),
          PercentC1Time = pyspark.sql.Row(value=c1),
          PercentC2Time = pyspark.sql.Row(value=c2),
          PercentC3Time = pyspark.sql.Row(value=c3),
          PercentIdleTime = pyspark.sql.Row(value=idle),
          PercentProcessorTime = pyspark.sql.Row(value=pt)
        )
      )
    
    return processor_info

  processors_schema = T.ArrayType.fromJson(original_schema["fields"][0]['type'])
  generate_processor_udf = F.udf(generate_processor, processors_schema)

  add_nest_field_df = (
    proto_df
    .withColumn(
      'Processors', 
      generate_processor_udf(F.col('num_processor'))
    )
    .drop('num_processor')
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
from array import ArrayType
import dbldatagen as dg
import pyspark.sql.types as T
import random

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path


random.seed(42)

# datatype_id = '652c6965-f001-4cf4-b980-90f42a502158'

# schema
# root
#  |-- ProcessorQueueLength: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- Threads: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- Processes: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- SystemUpTimeInSeconds: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- FileReadBytesPerSec: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- FileReadOperationsPerSec: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- FileWriteBytesPerSec: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- FileWriteOperationsPerSec: struct (nullable = true)
#  |    |-- value: long (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "ProcessorQueueLength",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Threads",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Processes",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "SystemUpTimeInSeconds",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FileReadBytesPerSec",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FileReadOperationsPerSec",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FileWriteBytesPerSec",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FileWriteOperationsPerSec",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "long", "nullable": True, "metadata": {}}
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
            "name": "ProcessorQueueLength",
            "type": "long",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Threads", "type": "long", "nullable": True, "metadata": {}},
        {"name": "Processes", "type": "long", "nullable": True, "metadata": {}},
        {
            "name": "SystemUpTimeInSeconds",
            "type": "long",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FileReadBytesPerSec",
            "type": "long",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FileReadOperationsPerSec",
            "type": "long",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FileWriteBytesPerSec",
            "type": "long",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FileWriteOperationsPerSec",
            "type": "long",
            "nullable": True,
            "metadata": {},
        },
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('OSSystemPerf', 'OSSystemPerf')

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
      'ProcessorQueueLength',
      minValue=1,
      maxValue=200,
      percentNulls=0.8
    )
    .withColumnSpec(
      'Threads',
      minValue=0,
      maxValue=10000    
    )
    .withColumnSpec(
      'Processes',
      minValue=0,
      maxValue=500  
    )
    .withColumnSpec(
      'SystemUpTimeInSeconds',
      minValue=0,
      maxValue=1000000  
    )
    .withColumnSpec(
      'FileReadBytesPerSec',
      minValue=0,
      maxValue=1000000000
    )
    .withColumnSpec(
      'FileReadOperationsPerSec',
      minValue=0,
      maxValue=200000 
    )
    .withColumnSpec(
      'FileWriteBytesPerSec',
      minValue=0,
      maxValue=10000000  
    )
    .withColumnSpec(
      'FileWriteOperationsPerSec',
      minValue=0,
      maxValue=10000 
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
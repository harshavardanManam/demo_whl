import dbldatagen as dg
import pyspark.sql.types as T

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path

# datatype_id = 'c9b162e7-c8e7-4e53-b338-295f6afad85d'

# schema
# root
#  |-- Manufacturer: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Name: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- SocketDesignation: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ProcessorId: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- MaxClockSpeedMHz: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- NumberOfCores: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- NumberOfEnabledCores: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- NumberOfLogicalProcessors: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- Generation: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- Level1CacheSizeInKb: array (nullable = true)
#  |    |-- element: integer (containsNull = false)
#  |-- Level2CacheSizeInKb: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- Level3CacheSizeInKb: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- NumberOfExtendedCores: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- NumberOfEnabledExtendedCores: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- NumberOfExtendedLogicalProcessors: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- Capability: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- value: string (nullable = true)
#  |-- BiosMicrocodeRevision: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- CurrentMicrocodeRevision: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- Description: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- CPUVProSupport: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
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
            "name": "SocketDesignation",
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
            "name": "ProcessorId",
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
            "name": "MaxClockSpeedMHz",
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
            "name": "NumberOfCores",
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
            "name": "NumberOfEnabledCores",
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
            "name": "NumberOfLogicalProcessors",
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
            "name": "Generation",
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
            "name": "Level1CacheSizeInKb",
            "type": {"type": "array", "elementType": "integer", "containsNull": False},
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Level2CacheSizeInKb",
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
            "name": "Level3CacheSizeInKb",
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
            "name": "NumberOfExtendedCores",
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
            "name": "NumberOfEnabledExtendedCores",
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
            "name": "NumberOfExtendedLogicalProcessors",
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
            "name": "BiosMicrocodeRevision",
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
            "name": "CurrentMicrocodeRevision",
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
        {
            "name": "CPUVProSupport",
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
    ],
}

current_schema = {
    "type": "struct",
    "fields": [
        {"name": "Manufacturer", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Name", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "SocketDesignation",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "ProcessorId", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "MaxClockSpeedMHz",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {"name": "NumberOfCores", "type": "integer", "nullable": True, "metadata": {}},
        {
            "name": "NumberOfEnabledCores",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "NumberOfLogicalProcessors",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Generation", "type": "integer", "nullable": True, "metadata": {}},
        {
            "name": "Level1CacheSizeInKb",
            "type": {"type": "array", "elementType": "integer", "containsNull": False},
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Level2CacheSizeInKb",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Level3CacheSizeInKb",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "NumberOfExtendedCores",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "NumberOfEnabledExtendedCores",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "NumberOfExtendedLogicalProcessors",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Capability",
            "type": {"type": "array", "elementType": "string", "containsNull": False},
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "BiosMicrocodeRevision",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "CurrentMicrocodeRevision",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Description", "type": "string", "nullable": True, "metadata": {}},
        {"name": "CPUVProSupport", "type": "boolean", "nullable": True, "metadata": {}},
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('CpuStaticInfo', 'CpuStaticInfo')

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
      'Manufacturer',
      values=['GenuineIntel']
    )
    .withColumnSpec(
      'Name',
      values=['11th Gen Intel(R) Core(TM) i5-1145G7 @ 2.60GHz', '11th Gen Intel(R) Core(TM) i9-11950H @ 2.60GHz', 'Intel(R) Core(TM) i5-10400H CPU @ 2.60GHz']
    )
    .withColumnSpec(
      'SocketDesignation',
      values=['U3E1']
    )
    .withColumnSpec(
      'ProcessorId',
      values=['BFEBFBFF000806C1', 'BFEBFBFF000806D1', 'BFEBFBFF000A0652']
    )
    .withColumnSpec(
      'MaxClockSpeedMHz',
      values=[1498, 2611, 2592]
    )
    .withColumnSpec(
      'NumberOfCores',
      baseColumn='ProcessorId',
      expr='''
      (
        case
          when ProcessorId = 'BFEBFBFF000806C1' or ProcessorId = 'BFEBFBFF000A0652' then 4
          when ProcessorId = 'BFEBFBFF000806D1' then 8
        end
      )
      '''    
    )
    .withColumnSpec(
      'NumberOfEnabledCores',
      baseColumn='ProcessorId',
      expr='''
      (
        case
          when ProcessorId = 'BFEBFBFF000806C1' or ProcessorId = 'BFEBFBFF000A0652' then 4
          when ProcessorId = 'BFEBFBFF000806D1' then 8
        end
      )
      '''      
    )
    .withColumnSpec(
      'NumberOfLogicalProcessors',
      baseColumn='ProcessorId',
      expr='''
      (
        case
          when ProcessorId = 'BFEBFBFF000806C1' or ProcessorId = 'BFEBFBFF000A0652' then 8
          when ProcessorId = 'BFEBFBFF000806D1' then 16
        end
      )
      '''      
    )
    .withColumnSpec(
      'Generation',
      baseColumn='ProcessorId',
      expr='''
      (
        case
          when ProcessorId = 'BFEBFBFF000806C1' or ProcessorId = 'BFEBFBFF000806D1' then 11
          when ProcessorId = 'BFEBFBFF000A0652' then 10
        end
      )
      '''  
    )
    .withColumnSpec(
      'Level1CacheSizeInKb',
      values=[None]
    )
    .withColumnSpec(
      'Level2CacheSizeInKb',
      values=[5120, 10240, 1024]
    )
    .withColumnSpec(
      'Level3CacheSizeInKb',
      baseColumn='ProcessorId',
      expr='''
      (
        case
          when ProcessorId = 'BFEBFBFF000806C1' or ProcessorId = 'BFEBFBFF000A0652' then 8192
          when ProcessorId = 'BFEBFBFF000806D1' then 24576
        end
      )
      ''' 
    )
    .withColumnSpec(
      'NumberOfExtendedCores',
      baseColumn='ProcessorId',
      expr='''
      (
        case
          when ProcessorId = 'BFEBFBFF000806C1' or ProcessorId = 'BFEBFBFF000A0652' then 4
          when ProcessorId = 'BFEBFBFF000806D1' then 8
        end
      )
      ''' 
    )
    .withColumnSpec(
      'NumberOfEnabledExtendedCores',
      baseColumn='ProcessorId',
      expr='''
      (
        case
          when ProcessorId = 'BFEBFBFF000806C1' or ProcessorId = 'BFEBFBFF000A0652' then 4
          when ProcessorId = 'BFEBFBFF000806D1' then 8
        end
      )
      ''' 
    )
    .withColumnSpec(
      'NumberOfExtendedLogicalProcessors',
      baseColumn='ProcessorId',
      expr='''
      (
        case
          when ProcessorId = 'BFEBFBFF000806C1' or ProcessorId = 'BFEBFBFF000A0652' then 8
          when ProcessorId = 'BFEBFBFF000806D1' then 16
        end
      )
      ''' 
    )
    .withColumnSpec(
      'Capability',
      values=[None]
    )
    .withColumnSpec(
      'BiosMicrocodeRevision',
      values=[134, 60, 240]
    )
    .withColumnSpec(
      'CurrentMicrocodeRevision',
      values=[134, 60, 240]
    )
    .withColumnSpec(
      'Description',
      values=['CPU']
    )
    .withColumnSpec(
      'CPUVProSupport',
      values=[True]
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )

  # Nested fields need to be added:
  # Level1CacheSizeInKb is array
  #  |-- Level1CacheSizeInKb: array (nullable = true)
  #  |    |-- element: integer (containsNull = false)

  # Capability is array
  #  |-- Capability: array (nullable = true)
  #  |    |-- element: struct (containsNull = false)
  #  |    |    |-- value: string (nullable = true)
  add_nest_field_df = (
    proto_df
    .withColumn(
      'Level1CacheSizeInKb',
      ( F
        .when(
          F.col('ProcessorId') == 'BFEBFBFF000A0652', 
          F.array(F.lit(256))
        )
        .when(
          F.col('ProcessorId') == 'BFEBFBFF000806C1', 
          F.array(F.lit(192), F.lit(128))
        )
        .when(
          F.col('ProcessorId') == 'BFEBFBFF000806D1', 
          F.array(F.lit(384), F.lit(256))
        )
      )
    )
    .withColumn(
      'Capability', 
      ( F
        .when(
          F.col('ProcessorId') == 'BFEBFBFF000A0652', 
          F.array(
            F.struct(F.lit('CompareExchangeDouble').alias('value')),
            F.struct(F.lit('MMXInstructionsAvailable').alias('value')),
            F.struct(F.lit('XMMIInstructionsAvailable').alias('value')),
            F.struct(F.lit('RDTSCInstructionAvailable').alias('value')),
            F.struct(F.lit('PAEEnabled').alias('value')),
            F.struct(F.lit('XMMI64InstructionsAvailable').alias('value')),
            F.struct(F.lit('NXEnabled').alias('value')),
            F.struct(F.lit('SSE3InstructionsAvailable').alias('value')),
            F.struct(F.lit('CompareExchange128').alias('value')),
            F.struct(F.lit('XSAVEEnabled').alias('value')),
            F.struct(F.lit('RdwrfsgsbaseAvailable').alias('value')),
            F.struct(F.lit('FastfailAvailable').alias('value')),
          )
        )
        .when(
          F.col('ProcessorId') == 'BFEBFBFF000806D1', 
          F.array(
            F.struct(F.lit('CompareExchangeDouble').alias('value')),
            F.struct(F.lit('MMXInstructionsAvailable').alias('value')),
            F.struct(F.lit('XMMIInstructionsAvailable').alias('value')),
            F.struct(F.lit('RDTSCInstructionAvailable').alias('value')),
            F.struct(F.lit('PAEEnabled').alias('value')),
            F.struct(F.lit('XMMI64InstructionsAvailable').alias('value')),
            F.struct(F.lit('NXEnabled').alias('value')),
            F.struct(F.lit('SSE3InstructionsAvailable').alias('value')),
            F.struct(F.lit('CompareExchange128').alias('value')),
            F.struct(F.lit('XSAVEEnabled').alias('value')),
            F.struct(F.lit('RdwrfsgsbaseAvailable').alias('value')),
            F.struct(F.lit('FastfailAvailable').alias('value')),
          )
        )
        .when(
          F.col('ProcessorId') == 'BFEBFBFF000806C1', 
          F.array(
            F.struct(F.lit('CompareExchangeDouble').alias('value')),
            F.struct(F.lit('MMXInstructionsAvailable').alias('value')),
            F.struct(F.lit('XMMIInstructionsAvailable').alias('value')),
            F.struct(F.lit('RDTSCInstructionAvailable').alias('value')),
            F.struct(F.lit('PAEEnabled').alias('value')),
            F.struct(F.lit('XMMI64InstructionsAvailable').alias('value')),
            F.struct(F.lit('NXEnabled').alias('value')),
            F.struct(F.lit('SSE3InstructionsAvailable').alias('value')),
            F.struct(F.lit('CompareExchange128').alias('value')),
            F.struct(F.lit('XSAVEEnabled').alias('value')),
            F.struct(F.lit('RdwrfsgsbaseAvailable').alias('value')),
            F.struct(F.lit('FastfailAvailable').alias('value')),
          )
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
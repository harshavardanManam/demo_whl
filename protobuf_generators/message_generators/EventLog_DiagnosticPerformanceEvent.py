import dbldatagen as dg
import pyspark.sql.types as T

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path


# datatype_id = '81423c33-8358-4ffd-8771-568094b919bf'

# schema
# root
#  |-- EventTime: timestamp (nullable = true)
#  |-- Severity: string (nullable = true)
#  |-- OriginalRecordId: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- EventID: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- EventDescription: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- StartTime: timestamp (nullable = true)
#  |-- DiagnosticsName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- DiagnosticsFriendlyName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- DiagnosticsVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- TotalTimeInMs: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- DurationInMs: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- IsDegradation: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- DegradationTimeInMs: struct (nullable = true)
#  |    |-- value: long (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {"name": "EventTime", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "Severity", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "OriginalRecordId",
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
            "name": "EventID",
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
            "name": "EventDescription",
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
        {"name": "StartTime", "type": "timestamp", "nullable": True, "metadata": {}},
        {
            "name": "DiagnosticsName",
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
            "name": "DiagnosticsFriendlyName",
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
            "name": "DiagnosticsVersion",
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
            "name": "TotalTimeInMs",
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
            "name": "DurationInMs",
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
            "name": "IsDegradation",
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
            "name": "DegradationTimeInMs",
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
        {"name": "EventTime", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "Severity", "type": "string", "nullable": True, "metadata": {}},
        {"name": "OriginalRecordId", "type": "long", "nullable": True, "metadata": {}},
        {"name": "EventID", "type": "integer", "nullable": True, "metadata": {}},
        {
            "name": "EventDescription",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "StartTime", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "DiagnosticsName", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "DiagnosticsFriendlyName",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DiagnosticsVersion",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "TotalTimeInMs", "type": "long", "nullable": True, "metadata": {}},
        {"name": "DurationInMs", "type": "long", "nullable": True, "metadata": {}},
        {"name": "IsDegradation", "type": "boolean", "nullable": True, "metadata": {}},
        {
            "name": "DegradationTimeInMs",
            "type": "long",
            "nullable": True,
            "metadata": {},
        },
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  population = 100000
  proto_file, message_name = ('EventLog', 'DiagnosticPerformanceEvent')

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
      'EventTime', 
      begin='2022-12-07 00:00:00', 
      end='2023-02-07 23:59:00', 
      interval='1 second'
    )
    .withColumnSpec(
      'Severity', 
      values=['Critical', 'Warning', 'Error']
    )
    .withColumnSpec(
      'OriginalRecordId',
      minValue=0,
      uniqueValues=population, 
      baseColumnType='hash'
    )
    .withColumnSpec(
      'EventID',
      minValue=100, 
      maxValue=999
    )
    .withColumnSpec(
      'EventDescription',
      text=dg.ILText(
        paragraphs=(1, 1),
        sentences=(2, 4)
      )
    )
    .withColumnSpec(
      'StartTime',
      baseColumn='EventTime',
      expr='cast((cast(EventTime as long) - 600) as timestamp)'
    )
    .withColumnSpec(
      'DiagnosticsName',
      template=r'\\w.e\xe'
    )
    .withColumnSpec(
      'DiagnosticsFriendlyName',
      text=dg.ILText(
        paragraphs=(1, 1),
        sentences=(1, 2)
      )
    )
    .withColumnSpec(
      'DiagnosticsVersion',
      template=r'\\n.\\n.\\n.\\n'
    )
    .withColumnSpec(
      'TotalTimeInMs',
      minValue=100000000,
      maxValue=200000000
    )
    .withColumnSpec(
      'DurationInMs',
      percentNulls=0.05,
      minValue=100000,
      maxValue=200000
    )
    .withColumnSpec(
      'IsDegradation',
      percentNulls=0.5,
      values=[True, False]
    )
    .withColumnSpec(
      'DegradationTimeInMs',
      percentNulls=0.1,
      minValue=100000,
      maxValue=200000
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )
  proto_original_df = set_nest_col(proto_df, original_schema)
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
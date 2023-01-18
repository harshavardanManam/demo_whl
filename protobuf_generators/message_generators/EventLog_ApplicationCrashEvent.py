from array import ArrayType
import dbldatagen as dg
import pyspark.sql.types as T
import random

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path


random.seed(42)

# datatype_id = 'c35cd409-7706-4204-8323-17452b9b86dd'

# schema
# root
#  |-- EventTime: timestamp (nullable = true)
#  |-- Severity: string (nullable = true)
#  |-- OriginalRecordId: struct (nullable = true)
#  |    |-- value: long (nullable = true)
#  |-- Name: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- AppVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- AppTimeStamp: timestamp (nullable = true)
#  |-- ModuleName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ModuleVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ModuleTimeStamp: timestamp (nullable = true)
#  |-- ExceptionCode: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- ExceptionString: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultOffset: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ProcessId: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingProcessName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingProcessVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingProcessDescription: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingProcessProductName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingProcessProductVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingModuleName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingModuleVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingModuleDescription: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingModuleProductName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FaultingModuleProductVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Base64Icon: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ReportId: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- PackageName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- PackageAppId: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- EventTimeBiasInMin: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- AppStartTimeStamp: timestamp (nullable = true)

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
            "name": "AppVersion",
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
        {"name": "AppTimeStamp", "type": "timestamp", "nullable": True, "metadata": {}},
        {
            "name": "ModuleName",
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
            "name": "ModuleVersion",
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
            "name": "ModuleTimeStamp",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "ExceptionCode",
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
            "name": "ExceptionString",
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
            "name": "FaultOffset",
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
            "name": "ProcessId",
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
            "name": "FaultingProcessName",
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
            "name": "FaultingProcessVersion",
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
            "name": "FaultingProcessDescription",
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
            "name": "FaultingProcessProductName",
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
            "name": "FaultingProcessProductVersion",
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
            "name": "FaultingModuleName",
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
            "name": "FaultingModuleVersion",
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
            "name": "FaultingModuleDescription",
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
            "name": "FaultingModuleProductName",
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
            "name": "FaultingModuleProductVersion",
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
            "name": "Base64Icon",
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
            "name": "ReportId",
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
            "name": "PackageName",
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
            "name": "PackageAppId",
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
            "name": "EventTimeBiasInMin",
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
            "name": "AppStartTimeStamp",
            "type": "timestamp",
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
        {"name": "Name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "AppVersion", "type": "string", "nullable": True, "metadata": {}},
        {"name": "AppTimeStamp", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "ModuleName", "type": "string", "nullable": True, "metadata": {}},
        {"name": "ModuleVersion", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "ModuleTimeStamp",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {"name": "ExceptionCode", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "ExceptionString", "type": "string", "nullable": True, "metadata": {}},
        {"name": "FaultOffset", "type": "string", "nullable": True, "metadata": {}},
        {"name": "ProcessId", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "FaultingProcessName",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingProcessVersion",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingProcessDescription",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingProcessProductName",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingProcessProductVersion",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingModuleName",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingModuleVersion",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingModuleDescription",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingModuleProductName",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "FaultingModuleProductVersion",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Base64Icon", "type": "string", "nullable": True, "metadata": {}},
        {"name": "ReportId", "type": "string", "nullable": True, "metadata": {}},
        {"name": "PackageName", "type": "string", "nullable": True, "metadata": {}},
        {"name": "PackageAppId", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "EventTimeBiasInMin",
            "type": "integer",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "AppStartTimeStamp",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('EventLog', 'ApplicationCrashEvent')

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
      interval='1 microsecond'
    )
    .withColumnSpec(
      'Severity',
      values=['Error']
    )
    .withColumnSpec(
      'OriginalRecordId',
      values=[64639, 64640, 66335, 66336, 66337, 66338, 67612, 67613, 67614, 67615, 70211, 151803, 157572]
    )
    .withColumnSpec(
      'Name',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then 'AwWindowsIpc.exe'
          when OriginalRecordId = 157572 then 'mfeatp.exe'
          else 'Dell.TechHub.Instrumentation.UserProcess.exe'
        end
      )
      '''
    )
    .withColumnSpec(
      'AppVersion',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then '10.7.0.3183'
          when OriginalRecordId = 157572 then '21.7.9.0'
          else '2.0.0.6001'
        end
      )
      '''
    )
    .withColumnSpec(
      'AppTimeStamp',
      baseColumn='EventTime',
      expr='cast((cast(EventTime as long) - rand(0) * 600) as timestamp)'
    )
    .withColumnSpec(
      'ModuleName',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then 'libuv.dll'
          else 'KERNELBASE.dll'
        end
      )
      '''    
    )
    .withColumnSpec(
      'ModuleVersion',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then '0.0.0.0'
          when OriginalRecordId = 157572 then '10.0.19041.2130'
          else '10.0.22000.1165'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'ModuleTimeStamp',
      baseColumn='EventTime',
      expr='cast((cast(EventTime as long) - rand(0) * 600) as timestamp)'
    )
    .withColumnSpec(
      'ExceptionCode',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then '-1073741819'
          else '-532462766'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'ExceptionString',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then 'The instruction referenced invalid memory.'
          when OriginalRecordId = 157572 then 'Some exception.'
          else 'Other exception.'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'FaultOffset',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then '000000000001fc25'
          when OriginalRecordId = 157572 then '0012df72'
          else '000000000004428c'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'ProcessId',
      expr='lower(hex(cast(rand(0) * 65535 as int)))'
    )
    .withColumnSpec(
      'FaultingProcessName',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then 'C:\Program Files\McAfee\Endpoint Security\Adaptive Threat Protection\mfeatp.exe'
          when OriginalRecordId = 157572 then 'C:\Program Files (x86)\Airwatch\AgentUI\AwWindowsIpc.exe'
          else 'C:\Program Files\Dell\DTP\InstrumentationSubAgent\Dell.TechHub.Instrumentation.UserProcess.exe'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'FaultingProcessVersion',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then '10.7.0.3183'
          when OriginalRecordId = 157572 then '21.7.9.0'
          else '2.0.0.6001'
        end
      )
      '''
    )
    .withColumnSpec(
      'FaultingProcessDescription',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then 'McAfee Adaptive Threat Protection Service'
          when OriginalRecordId = 157572 then 'AwWindowsIpc'
          else 'Dell Instrumentation\\'s User Process'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'FaultingProcessProductName',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then 'McAfee Endpoint Security'
          when OriginalRecordId = 157572 then 'AwWindowsIpc'
          else 'Dell Instrumentation\\'s User Process'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'FaultingProcessProductVersion',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then '10.7.0'
          when OriginalRecordId = 157572 then '21.7.9.0'
          else '2.0.0.6001'
        end
      )
      '''
    )
    .withColumnSpec(
      'FaultingModuleName',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then 'C:\Program Files\McAfee\Agent\libuv.dll'
          else 'C:\WINDOWS\System32\KERNELBASE.dll'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'FaultingModuleVersion',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then '10.0.22000.708 (WinBuild.160101.0800)'
          when OriginalRecordId = 157572 then null
          else '10.0.19041.1741 (WinBuild.160101.0800)'
        end
      )
      '''
    )
    .withColumnSpec(
      'FaultingModuleDescription',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then null
          else 'Windows NT BASE API Client DLL'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'FaultingModuleProductName',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then null
          else 'Microsoft® Windows® Operating System'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'FaultingModuleProductVersion',
      baseColumn='OriginalRecordId',
      expr='''
      (
        case
          when OriginalRecordId = 151803 then 'null'
          when OriginalRecordId = 157572 then '10.0.19041.1741'
          else '10.0.22000.708'
        end
      )
      ''' 
    )
    .withColumnSpec(
      'Base64Icon',
      values=['AAABAAEAICAQJwAAAADoAgAAFgAAACgAAAAgAAAAQAAAAAEABAAAAAAAgAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAACAAAAAgIAAgAAAAIAAgACAgAAAgICAAMDAwAAAAP8AAP8AAAD//wD/AAAA/wD/AP//AAD///8A//////////////////////////////////////////////////////////////////////////Zv//////////////////ZmZm////////////////ZmZmZmj/////////////9mZmZmZmb///////////9mZmZmZmZmZv////////9mZmZmZmZmZmZm////////ZmZmZm/2ZmZmZv////////9mZmj//2ZmZv//////////9mb/////Zo////////9mj///////////+Gb/////Zmb//////////2Zm/////2Zmb/////////ZmZv////9mZm/////////2Zmb/////ZmZv////////9mZm/////2Zmb/////////ZmZv////9mZm/////////2Zmb/////ZmZv////////9mZm/////2Zmb/////////ZmZv////9mZmZv//////ZmZmb/////ZmZmZm////ZmZmZm/////2ZmZmZm//hmZmZmZv//////ZmZmZm/2ZmZmZv////////hmZmZv9mZmZv//////////9mZmb/ZmZm/////////////2Zmj2Zm////////////////Zv9m////////////////////////////////////////////////////////////////////////8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==']
    )
    .withColumnSpec(
      'ReportId',
      template='xxxxxxxx-xxxx-xxxx-xxxxxxxxxxxx'
    )
    .withColumnSpec(
      'PackageName',
      values=[None]
    )
    .withColumnSpec(
      'PackageAppId',
      values=[None]
    )
    .withColumnSpec(
      'EventTimeBiasInMin',
      minValue=-360,
      maxValue=360
    )
    .withColumnSpec(
      'AppStartTimeStamp',
      baseColumn='EventTime',
      expr='cast((cast(EventTime as long) - rand(0) * 600) as timestamp)'
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
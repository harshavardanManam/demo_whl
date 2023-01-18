import dbldatagen as dg
import pyspark.sql.types as T

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path

# datatype_id = 'b9bdc4e0-3d19-4cd0-941c-b51332d4a87f'

# schema
# root
#  |-- OSVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Name: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- EditionID: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- OSArchitecture: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- BuildBranch: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Codename: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- MarketName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ReleaseId: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Version: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- CurrentMajorVersionNumber: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- CurrentMinorVersionNumber: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- CurrentVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- CurrentBuildNumber: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- UpdateBuildRevision: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- MUILanguage: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- value: string (nullable = true)
#  |-- OSLanguage: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Locale: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ServicePackMajorVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ServicePackMinorVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- SystemDirectory: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- WindowsDirectory: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Manufacturer: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ProductID: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- LocaleName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Countrycode: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- CountryName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- Platform: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- MaximumOsMemoryInGigabytes: struct (nullable = true)
#  |    |-- value: float (nullable = true)
#  |-- IsUserAccessControlDisabled: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsSystemRestorePointEnabled: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsWindowsDefragmentEnabled: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsWindowsDefragmentScheduled: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- LastDefragmentTime: timestamp (nullable = true)
#  |-- NextDefragmentTime: timestamp (nullable = true)
#  |-- UserLocale: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- UserLocaleName: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- KernelVersion: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- KernelArchitecture: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- OSDistribution: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- FileHistoryBackup: struct (nullable = true)
#  |    |-- value: string (nullable = true)
#  |-- ScreenSaverEnabled: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- IsWindowsFirewallEnabled: struct (nullable = true)
#  |    |-- value: boolean (nullable = true)
#  |-- Capability: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- value: string (nullable = true)
#  |-- GeoLocationId: struct (nullable = true)
#  |    |-- value: integer (nullable = true)
#  |-- OobeDate: timestamp (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "OSVersion",
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
            "name": "EditionID",
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
            "name": "OSArchitecture",
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
            "name": "BuildBranch",
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
            "name": "Codename",
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
            "name": "MarketName",
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
            "name": "ReleaseId",
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
            "name": "CurrentMajorVersionNumber",
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
            "name": "CurrentMinorVersionNumber",
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
            "name": "CurrentVersion",
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
            "name": "CurrentBuildNumber",
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
            "name": "UpdateBuildRevision",
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
            "name": "MUILanguage",
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
            "name": "OSLanguage",
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
            "name": "Locale",
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
            "name": "ServicePackMajorVersion",
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
            "name": "ServicePackMinorVersion",
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
            "name": "SystemDirectory",
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
            "name": "WindowsDirectory",
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
            "name": "ProductID",
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
            "name": "LocaleName",
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
            "name": "Countrycode",
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
            "name": "CountryName",
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
            "name": "Platform",
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
            "name": "MaximumOsMemoryInGigabytes",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "float", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsUserAccessControlDisabled",
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
            "name": "IsSystemRestorePointEnabled",
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
            "name": "IsWindowsDefragmentEnabled",
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
            "name": "IsWindowsDefragmentScheduled",
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
            "name": "LastDefragmentTime",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "NextDefragmentTime",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "UserLocale",
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
            "name": "UserLocaleName",
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
            "name": "KernelVersion",
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
            "name": "KernelArchitecture",
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
            "name": "OSDistribution",
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
            "name": "FileHistoryBackup",
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
            "name": "ScreenSaverEnabled",
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
            "name": "IsWindowsFirewallEnabled",
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
            "name": "GeoLocationId",
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
        {"name": "OobeDate", "type": "timestamp", "nullable": True, "metadata": {}},
    ],
}

current_schema = {
    "type": "struct",
    "fields": [
        {"name": "OSVersion", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "EditionID", "type": "string", "nullable": True, "metadata": {}},
        {"name": "OSArchitecture", "type": "string", "nullable": True, "metadata": {}},
        {"name": "BuildBranch", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Codename", "type": "string", "nullable": True, "metadata": {}},
        {"name": "MarketName", "type": "string", "nullable": True, "metadata": {}},
        {"name": "ReleaseId", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Version", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "CurrentMajorVersionNumber",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "CurrentMinorVersionNumber",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "CurrentVersion", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "CurrentBuildNumber",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "UpdateBuildRevision",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "MUILanguage",
            "type": {"type": "array", "elementType": "string", "containsNull": False},
            "nullable": True,
            "metadata": {},
        },
        {"name": "OSLanguage", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Locale", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "ServicePackMajorVersion",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "ServicePackMinorVersion",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "SystemDirectory", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "WindowsDirectory",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "Manufacturer", "type": "string", "nullable": True, "metadata": {}},
        {"name": "ProductID", "type": "string", "nullable": True, "metadata": {}},
        {"name": "LocaleName", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Countrycode", "type": "string", "nullable": True, "metadata": {}},
        {"name": "CountryName", "type": "string", "nullable": True, "metadata": {}},
        {"name": "Platform", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "MaximumOsMemoryInGigabytes",
            "type": "float",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsUserAccessControlDisabled",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsSystemRestorePointEnabled",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsWindowsDefragmentEnabled",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsWindowsDefragmentScheduled",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "LastDefragmentTime",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "NextDefragmentTime",
            "type": "timestamp",
            "nullable": True,
            "metadata": {},
        },
        {"name": "UserLocale", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "UserLocaleName", "type": "string", "nullable": True, "metadata": {}},
        {"name": "KernelVersion", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "KernelArchitecture",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {"name": "OSDistribution", "type": "string", "nullable": True, "metadata": {}},
        {
            "name": "FileHistoryBackup",
            "type": "string",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "ScreenSaverEnabled",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "IsWindowsFirewallEnabled",
            "type": "boolean",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Capability",
            "type": {"type": "array", "elementType": "string", "containsNull": False},
            "nullable": True,
            "metadata": {},
        },
        {"name": "GeoLocationId", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "OobeDate", "type": "timestamp", "nullable": True, "metadata": {}},
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('OperatingSystemInfo', 'OperatingSystemInfo')

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
      'OSVersion',
      values=['Microsoft Windows 10 Enterprise[64-bit OS][System Locale en-US]', 'Microsoft Windows 11 Enterprise[64-bit OS][System Locale en-US]']
    )
    .withColumnSpec(
      'Name',
      values=['Microsoft Windows 10 Enterprise', 'Microsoft Windows 11 Enterprise']
    )
    .withColumnSpec(
      'EditionID',
      values=['Enterprise']
    )
    .withColumnSpec(
      'OSArchitecture',
      values=['64-bit']
    )
    .withColumnSpec(
      'BuildBranch',
      values=['vb_release', 'co_release']
    )
    .withColumnSpec(
      'Codename',
      values=['20H2', '21H2']
    )
    .withColumnSpec(
      'MarketName',
      values=['October 2020 Update', 'Windows 11']
    )
    .withColumnSpec(
      'ReleaseId',
      values=['2009']
    )
    .withColumnSpec(
      'Version',
      values=['10.0.19042', '10.0.22000']
    )
    .withColumnSpec(
      'CurrentMajorVersionNumber',
      values=['10']
    )
    .withColumnSpec(
      'CurrentMinorVersionNumber',
      values=['0']
    )
    .withColumnSpec(
      'CurrentVersion',
      values=['10.0']
    )
    .withColumnSpec(
      'CurrentBuildNumber',
      values=['19042', '22000']
    )
    .withColumnSpec(
      'UpdateBuildRevision',
      values=['2130', '1219']
    )
    .withColumnSpec(
      'MUILanguage',
      values=[None]
    )
    .withColumnSpec(
      'OSLanguage',
      values=['1033']
    )
    .withColumnSpec(
      'Locale',
      values=['0409']
    )
    .withColumnSpec(
      'ServicePackMajorVersion',
      values=['0']
    )
    .withColumnSpec(
      'ServicePackMinorVersion',
      values=['0']
    )
    .withColumnSpec(
      'SystemDirectory',
      values=['C:\WINDOWS\system32']
    )
    .withColumnSpec(
      'WindowsDirectory',
      values=['C:\WINDOWS']
    )
    .withColumnSpec(
      'Manufacturer',
      values=['Microsoft Corporation']
    )
    .withColumnSpec(
      'ProductID',
      values=['00329-00000-00003-AA278', '00330-80000-00000-AA758', '00330-80000-00000-AA859', '00330-80000-00000-AA940']
    )
    .withColumnSpec(
      'LocaleName',
      values=['English (United States)']
    )
    .withColumnSpec(
      'Countrycode',
      values=['US']
    )
    .withColumnSpec(
      'CountryName',
      values=['United States']
    )
    .withColumnSpec(
      'Platform',
      values=['Win32NT']
    )
    .withColumnSpec(
      'MaximumOsMemoryInGigabytes',
      values=[16, 64]
    )
    .withColumnSpec(
      'IsUserAccessControlDisabled',
      values=[None]
    )
    .withColumnSpec(
      'IsSystemRestorePointEnabled',
      values=[True, None]
    )
    .withColumnSpec(
      'IsWindowsDefragmentEnabled',
      values=[True]
    )
    .withColumnSpec(
      'IsWindowsDefragmentScheduled',
      values=[None]
    )
    .withColumnSpec(
      'LastDefragmentTime',
      begin='2022-12-07 00:00:00', 
      end='2023-02-07 23:59:00', 
      interval='1 minute'
    )
    .withColumnSpec(
      'NextDefragmentTime',
      values=[None]
    )
    .withColumnSpec(
      'UserLocale',
      values=[1033]
    )
    .withColumnSpec(
      'UserLocaleName',
      values=['English (United States)']
    )
    .withColumnSpec(
      'KernelVersion',
      values=['10.0']
    )
    .withColumnSpec(
      'KernelArchitecture',
      values=['x64']
    )
    .withColumnSpec(
      'OSDistribution',
      values=['Enterprise Edition']
    )
    .withColumnSpec(
      'FileHistoryBackup',
      values=['Disabled']
    )
    .withColumnSpec(
      'ScreenSaverEnabled',
      values=[None]
    )
    .withColumnSpec(
      'IsWindowsFirewallEnabled',
      values=[True]
    )
    .withColumnSpec(
      'Capability',
      values=[None]
    )
    .withColumnSpec(
      'GeoLocationId',
      values=[244]
    )
    .withColumnSpec(
      'OobeDate',
      baseColumn='LastDefragmentTime',
      expr='date_sub(LastDefragmentTime, cast(rand(0) * 365 as int))'
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )

  # Nested fields need to be added:
  # MUILanguage is array
  #  |-- MUILanguage: array (nullable = true)
  #  |    |-- element: struct (containsNull = false)
  #  |    |    |-- value: string (nullable = true)

  # Capability is array
  #  |-- Capability: array (nullable = true)
  #  |    |-- element: struct (containsNull = false)
  #  |    |    |-- value: string (nullable = true)
  add_nest_field_df = (
    proto_df
    .withColumn(
      'MUILanguage',
      ( F
        .when(
          F.col('id') % 100 == 0, 
          F.array(
            F.struct(F.lit('en-US').alias('value')),
          )
        )
        .otherwise(
          F.array(
            F.struct(F.lit('en-US').alias('value')),
            F.struct(F.lit('de-DE').alias('value')),
            F.struct(F.lit('ja-JP').alias('value')),
            F.struct(F.lit('zh-CN').alias('value')),
          )
        )
      )
    )
    .withColumn(
      'Capability', 
      F.array(
        F.struct(F.lit('OperatingSystem').alias('value')),
        F.struct(F.lit('Software').alias('value')),
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
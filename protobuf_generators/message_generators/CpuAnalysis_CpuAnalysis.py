from array import ArrayType
import dbldatagen as dg
import pyspark.sql.types as T
import random

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path


random.seed(42)

# datatype_id = '4136acd9-b69f-4b6d-96e7-233932755b33'

# schema
# root
#  |-- TotalPhysicalProcessor: struct (nullable = true)
#  |    |-- UsageStatistics: struct (nullable = true)
#  |    |    |-- Average: struct (nullable = true)
#  |    |    |    |-- value: float (nullable = true)
#  |    |    |-- Maximum: struct (nullable = true)
#  |    |    |    |-- value: float (nullable = true)
#  |    |    |-- Minimum: struct (nullable = true)
#  |    |    |    |-- value: float (nullable = true)
#  |    |    |-- StandardDeviation: struct (nullable = true)
#  |    |    |    |-- value: float (nullable = true)
#  |    |    |-- Latest: struct (nullable = true)
#  |    |    |    |-- value: float (nullable = true)
#  |    |    |-- Mode: array (nullable = false)
#  |    |    |    |-- element: struct (containsNull = false)
#  |    |    |    |    |-- value: float (nullable = true)
#  |    |-- C0StatePercentageAverage: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- C1StatePercentageAverage: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- C2StatePercentageAverage: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- C3StatePercentageAverage: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Usage0InPct: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Usage1To20InPct: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Usage21To40InPct: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Usage41To60InPct: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Usage61To80InPct: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Usage81To100InPct: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |-- LogicalProcessors: array (nullable = true)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- Name: struct (nullable = true)
#  |    |    |    |-- value: string (nullable = true)
#  |    |    |-- AverageCpuUsed: struct (nullable = true)
#  |    |    |    |-- value: float (nullable = true)
#  |    |    |-- UsageStatistics: struct (nullable = true)
#  |    |    |    |-- Average: struct (nullable = true)
#  |    |    |    |    |-- value: float (nullable = true)
#  |    |    |    |-- Maximum: struct (nullable = true)
#  |    |    |    |    |-- value: float (nullable = true)
#  |    |    |    |-- Minimum: struct (nullable = true)
#  |    |    |    |    |-- value: float (nullable = true)
#  |    |    |    |-- StandardDeviation: struct (nullable = true)
#  |    |    |    |    |-- value: float (nullable = true)
#  |    |    |    |-- Latest: struct (nullable = true)
#  |    |    |    |    |-- value: float (nullable = true)
#  |    |    |    |-- Mode: array (nullable = false)
#  |    |    |    |    |-- element: struct (containsNull = false)
#  |    |    |    |    |    |-- value: float (nullable = true)
#  |-- ThrottleStatistics: struct (nullable = true)
#  |    |-- Average: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Maximum: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Minimum: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- StandardDeviation: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Latest: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- Mode: array (nullable = false)
#  |    |    |-- element: struct (containsNull = false)
#  |    |    |    |-- value: float (nullable = true)
#  |-- Throttle0InPct: struct (nullable = true)
#  |    |-- value: float (nullable = true)
#  |-- Throttle1To25InPct: struct (nullable = true)
#  |    |-- value: float (nullable = true)
#  |-- Throttle26To50InPct: struct (nullable = true)
#  |    |-- value: float (nullable = true)
#  |-- Throttle51To75InPct: struct (nullable = true)
#  |    |-- value: float (nullable = true)
#  |-- Throttle76To100InPct: struct (nullable = true)
#  |    |-- value: float (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "TotalPhysicalProcessor",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "UsageStatistics",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "Average",
                                    "type": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "value",
                                                "type": "float",
                                                "nullable": True,
                                                "metadata": {},
                                            }
                                        ],
                                    },
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "Maximum",
                                    "type": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "value",
                                                "type": "float",
                                                "nullable": True,
                                                "metadata": {},
                                            }
                                        ],
                                    },
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "Minimum",
                                    "type": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "value",
                                                "type": "float",
                                                "nullable": True,
                                                "metadata": {},
                                            }
                                        ],
                                    },
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "StandardDeviation",
                                    "type": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "value",
                                                "type": "float",
                                                "nullable": True,
                                                "metadata": {},
                                            }
                                        ],
                                    },
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "Latest",
                                    "type": {
                                        "type": "struct",
                                        "fields": [
                                            {
                                                "name": "value",
                                                "type": "float",
                                                "nullable": True,
                                                "metadata": {},
                                            }
                                        ],
                                    },
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "Mode",
                                    "type": {
                                        "type": "array",
                                        "elementType": {
                                            "type": "struct",
                                            "fields": [
                                                {
                                                    "name": "value",
                                                    "type": "float",
                                                    "nullable": True,
                                                    "metadata": {},
                                                }
                                            ],
                                        },
                                        "containsNull": False,
                                    },
                                    "nullable": False,
                                    "metadata": {},
                                },
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "C0StatePercentageAverage",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "C1StatePercentageAverage",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "C2StatePercentageAverage",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "C3StatePercentageAverage",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage0InPct",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage1To20InPct",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage21To40InPct",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage41To60InPct",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage61To80InPct",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage81To100InPct",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
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
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "LogicalProcessors",
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
                            "name": "AverageCpuUsed",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "value",
                                        "type": "float",
                                        "nullable": True,
                                        "metadata": {},
                                    }
                                ],
                            },
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "UsageStatistics",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "Average",
                                        "type": {
                                            "type": "struct",
                                            "fields": [
                                                {
                                                    "name": "value",
                                                    "type": "float",
                                                    "nullable": True,
                                                    "metadata": {},
                                                }
                                            ],
                                        },
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "Maximum",
                                        "type": {
                                            "type": "struct",
                                            "fields": [
                                                {
                                                    "name": "value",
                                                    "type": "float",
                                                    "nullable": True,
                                                    "metadata": {},
                                                }
                                            ],
                                        },
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "Minimum",
                                        "type": {
                                            "type": "struct",
                                            "fields": [
                                                {
                                                    "name": "value",
                                                    "type": "float",
                                                    "nullable": True,
                                                    "metadata": {},
                                                }
                                            ],
                                        },
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "StandardDeviation",
                                        "type": {
                                            "type": "struct",
                                            "fields": [
                                                {
                                                    "name": "value",
                                                    "type": "float",
                                                    "nullable": True,
                                                    "metadata": {},
                                                }
                                            ],
                                        },
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "Latest",
                                        "type": {
                                            "type": "struct",
                                            "fields": [
                                                {
                                                    "name": "value",
                                                    "type": "float",
                                                    "nullable": True,
                                                    "metadata": {},
                                                }
                                            ],
                                        },
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "Mode",
                                        "type": {
                                            "type": "array",
                                            "elementType": {
                                                "type": "struct",
                                                "fields": [
                                                    {
                                                        "name": "value",
                                                        "type": "float",
                                                        "nullable": True,
                                                        "metadata": {},
                                                    }
                                                ],
                                            },
                                            "containsNull": False,
                                        },
                                        "nullable": False,
                                        "metadata": {},
                                    },
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
            "name": "ThrottleStatistics",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "Average",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Maximum",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Minimum",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "StandardDeviation",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Latest",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "value",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Mode",
                        "type": {
                            "type": "array",
                            "elementType": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "value",
                                        "type": "float",
                                        "nullable": True,
                                        "metadata": {},
                                    }
                                ],
                            },
                            "containsNull": False,
                        },
                        "nullable": False,
                        "metadata": {},
                    },
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Throttle0InPct",
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
            "name": "Throttle1To25InPct",
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
            "name": "Throttle26To50InPct",
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
            "name": "Throttle51To75InPct",
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
            "name": "Throttle76To100InPct",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "value", "type": "float", "nullable": True, "metadata": {}}
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
            "name": "TotalPhysicalProcessor",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "UsageStatistics",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "Average",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "Maximum",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "Minimum",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "StandardDeviation",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "Latest",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {},
                                },
                                {
                                    "name": "Mode",
                                    "type": {
                                        "type": "array",
                                        "elementType": "float",
                                        "containsNull": False,
                                    },
                                    "nullable": True,
                                    "metadata": {},
                                },
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "C0StatePercentageAverage",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "C1StatePercentageAverage",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "C2StatePercentageAverage",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "C3StatePercentageAverage",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage0InPct",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage1To20InPct",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage21To40InPct",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage41To60InPct",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage61To80InPct",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Usage81To100InPct",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "LogicalProcessors",
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
                            "name": "AverageCpuUsed",
                            "type": "float",
                            "nullable": True,
                            "metadata": {},
                        },
                        {
                            "name": "UsageStatistics",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "Average",
                                        "type": "float",
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "Maximum",
                                        "type": "float",
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "Minimum",
                                        "type": "float",
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "StandardDeviation",
                                        "type": "float",
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "Latest",
                                        "type": "float",
                                        "nullable": True,
                                        "metadata": {},
                                    },
                                    {
                                        "name": "Mode",
                                        "type": {
                                            "type": "array",
                                            "elementType": "float",
                                            "containsNull": False,
                                        },
                                        "nullable": False,
                                        "metadata": {},
                                    },
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
            "name": "ThrottleStatistics",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "Average",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Maximum",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Minimum",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "StandardDeviation",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Latest",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "Mode",
                        "type": {
                            "type": "array",
                            "elementType": "float",
                            "containsNull": False,
                        },
                        "nullable": True,
                        "metadata": {},
                    },
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {"name": "Throttle0InPct", "type": "float", "nullable": True, "metadata": {}},
        {
            "name": "Throttle1To25InPct",
            "type": "float",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Throttle26To50InPct",
            "type": "float",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Throttle51To75InPct",
            "type": "float",
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "Throttle76To100InPct",
            "type": "float",
            "nullable": True,
            "metadata": {},
        },
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('CpuAnalysis', 'CpuAnalysis')

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
      'TotalPhysicalProcessor',
      values=[None]
    )
    .withColumnSpec(
      'LogicalProcessors',
      values=[None]
    )
    .withColumnSpec(
      'ThrottleStatistics',
      values=[None]
    )
    .withColumnSpec(
      'Throttle0InPct',
      values=[100]
    )
    .withColumnSpec(
      'Throttle1To25InPct',
      values=[None]
    )
    .withColumnSpec(
      'Throttle26To50InPct',
      values=[None]
    )
    .withColumnSpec(
      'Throttle51To75InPct',
      values=[None]
    )
    .withColumnSpec(
      'Throttle76To100InPct',
      values=[None]
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )


  # UsageStatistics: struct (nullable = true)
  #  |-- Average: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Maximum: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Minimum: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- StandardDeviation: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Latest: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Mode: array (nullable = false)
  #  |    |-- element: struct (containsNull = false)
  #  |    |    |-- value: float (nullable = true)
  def generate_UsageStatistics() -> pyspark.sql.Row:
    maximum = random.randint(0, 100)
    minimum = random.randint(0, maximum)
    average = random.randint(minimum, maximum)
    sd = None
    latest = None
    mode = None

    if minimum == 0: minimum = None
    if maximum == 0: maximum = None
    if average == 0: average = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average),
      Maximum = pyspark.sql.Row(value=maximum),
      Minimum = pyspark.sql.Row(value=minimum),
      StandardDeviation = pyspark.sql.Row(value=sd),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  # TotalPhysicalProcessor: struct (nullable = true)
  #  |-- UsageStatistics: struct (nullable = true)
  #  |-- C0StatePercentageAverage: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- C1StatePercentageAverage: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- C2StatePercentageAverage: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- C3StatePercentageAverage: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Usage0InPct: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Usage1To20InPct: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Usage21To40InPct: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Usage41To60InPct: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Usage61To80InPct: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Usage81To100InPct: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  def generate_TotalPhysicalProcessor() -> pyspark.sql.Row:
    c0 = random.randint(0, 100)
    rest = 100 - c0
    c1 = random.randint(0, rest)
    rest -= c1
    c2 = random.randint(0, rest)
    rest -= c2
    c3 = random.randint(0, rest)

    usage0 = random.randint(0, 100)
    rest = 100 - usage0
    usage1 = random.randint(0, rest)
    rest -= usage1
    usage2 = random.randint(0, rest)
    rest -= usage2
    usage4 = random.randint(0, rest)
    rest -= usage4
    usage6 = random.randint(0, rest)
    rest -= usage6
    usage8 = random.randint(0, rest)

    return pyspark.sql.Row(
      UsageStatistics = generate_UsageStatistics(),
      C0StatePercentageAverage = pyspark.sql.Row(value=c0 if c0 != 0 else None),
      C1StatePercentageAverage = pyspark.sql.Row(value=c1 if c1 != 0 else None),
      C2StatePercentageAverage = pyspark.sql.Row(value=c2 if c2 != 0 else None),
      C3StatePercentageAverage = pyspark.sql.Row(value=c3 if c3 != 0 else None),
      Usage0InPct = pyspark.sql.Row(value=usage0 if usage0 != 0 else None),
      Usage1To20InPct = pyspark.sql.Row(value=usage1 if usage1 != 0 else None),
      Usage21To40InPct = pyspark.sql.Row(value=usage2 if usage2 != 0 else None),
      Usage41To60InPct = pyspark.sql.Row(value=usage4 if usage4 != 0 else None),
      Usage61To80InPct = pyspark.sql.Row(value=usage6 if usage6 != 0 else None),
      Usage81To100InPct = pyspark.sql.Row(value=usage8 if usage8 != 0 else None)
    )


  # LogicalProcessors: array (nullable = true)
  #  |-- element: struct (containsNull = false)
  #  |    |-- Name: struct (nullable = true)
  #  |    |    |-- value: string (nullable = true)
  #  |    |-- AverageCpuUsed: struct (nullable = true)
  #  |    |    |-- value: float (nullable = true)
  #  |    |-- UsageStatistics: struct (nullable = true)
  def generate_LogicalProcessors() -> list:
    logical_processors = []
    num_processors = 8 if random.random() < 0.5 else 16
    for i in range(num_processors):
      logical_processors.append(
        pyspark.sql.Row(
          Name = pyspark.sql.Row(value=f'Processor{i}'),
          AverageCpuUsed = pyspark.sql.Row(value=random.randint(0, 100)),
          UsageStatistics = generate_UsageStatistics()
        )
      )

    return logical_processors


  # ThrottleStatistics: struct (nullable = true)
  #  |-- Average: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Maximum: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Minimum: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- StandardDeviation: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Latest: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- Mode: array (nullable = false)
  #  |    |-- element: struct (containsNull = false)
  #  |    |    |-- value: float (nullable = true)
  def generate_ThrottleStatistics() -> pyspark.sql.Row:
    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=None),
      Maximum = pyspark.sql.Row(value=None),
      Minimum = pyspark.sql.Row(value=None),
      StandardDeviation = pyspark.sql.Row(value=None),
      Latest = pyspark.sql.Row(value=None),
      Mode = [pyspark.sql.Row(value=None)]
    )


  # Nested fields need to be added:
  # TotalPhysicalProcessor is struct
  # LogicalProcessors is array
  # ThrottleStatistics is struct

  TotalPhysicalProcessor_schema = T.StructType.fromJson(original_schema["fields"][0]['type'])
  LogicalProcessors_schema = T.ArrayType.fromJson(original_schema["fields"][1]['type'])
  ThrottleStatistics_schema = T.StructType.fromJson(original_schema["fields"][2]['type'])

  generate_TotalPhysicalProcessor_udf = F.udf(generate_TotalPhysicalProcessor, TotalPhysicalProcessor_schema)
  generate_LogicalProcessors_udf = F.udf(generate_LogicalProcessors, LogicalProcessors_schema)
  generate_ThrottleStatistics_udf = F.udf(generate_ThrottleStatistics, ThrottleStatistics_schema)

  add_nest_field_df = (
    proto_df
    .withColumn(
      'TotalPhysicalProcessor', 
      generate_TotalPhysicalProcessor_udf()
    )
    .withColumn(
      'LogicalProcessors', 
      generate_LogicalProcessors_udf()
    )
    .withColumn(
      'ThrottleStatistics', 
      generate_ThrottleStatistics_udf()
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
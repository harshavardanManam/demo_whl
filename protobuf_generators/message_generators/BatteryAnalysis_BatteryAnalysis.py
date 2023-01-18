from array import ArrayType
import dbldatagen as dg
import pyspark.sql.types as T
import random

from protobuf_generators.message_generators.helpers import *
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import to_protobuf
from proto_ingestion.utils.helpers import get_desc_path


random.seed(42)

# datatype_id = '11036c82-3669-48c3-80ce-00aad3e121b4'

# schema
# root
#  |-- ChargeData: struct (nullable = true)
#  |    |-- ChargeTimeInMinutes: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- FullChargeSessionNumber: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- PartialChargeSessionNumber: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- ChargeTemperaturInKelvin: struct (nullable = true)
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
#  |    |-- ChargeCurrent: struct (nullable = true)
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
#  |    |-- ChargeVoltage: struct (nullable = true)
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
#  |    |-- ChargePowerWhenRSOCGreaterThan60: struct (nullable = true)
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
#  |    |-- ChargePowerWhenRSOCLessThanOrEqualTo60: struct (nullable = true)
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
#  |-- DwellData: struct (nullable = true)
#  |    |-- DwellTimeInMinutes: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DwellRSOCLevelAverage: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DwellTemperaturInKelvin: struct (nullable = true)
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
#  |-- DischargeData: struct (nullable = true)
#  |    |-- DischargeTimeInMinutes: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargeTemperaturInKelvin: struct (nullable = true)
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
#  |    |-- DischargeCurrent: struct (nullable = true)
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
#  |    |-- DischargeVoltage: struct (nullable = true)
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
#  |    |-- DischargePower: struct (nullable = true)
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
#  |    |-- DischargeDepthSessionGt80: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeDepthSession60to80: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeDepthSession40to60: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeDepthSession20to40: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeDepthSession10to20: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeDepthSession5to10: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeDepthSessionLe5: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeStartLevelSessionGt94: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeStartLevelSessionGt70to94: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeStartLevelSessionGt50to70: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeStartLevelSessionGt30to50: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeStartLevelSessionGt10to30: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeStartLevelSessionLe10: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeEndLevelSession10to15: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeEndLevelSession5to10: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargeEndLevelSessionLt5: struct (nullable = true)
#  |    |    |-- value: integer (nullable = true)
#  |    |-- DischargePowerPercentageGe60: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentage50to60: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentage40to50: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentage30to40: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentage25to30: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentage20to25: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentage15to20: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentage10to15: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentage5to10: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |    |-- DischargePowerPercentageLt5: struct (nullable = true)
#  |    |    |-- value: float (nullable = true)
#  |-- CycleCount: struct (nullable = true)
#  |    |-- value: integer (nullable = true)

original_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "ChargeData",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "ChargeTimeInMinutes",
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
                        "name": "FullChargeSessionNumber",
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
                        "name": "PartialChargeSessionNumber",
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
                        "name": "ChargeTemperaturInKelvin",
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
                        "name": "ChargeCurrent",
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
                        "name": "ChargeVoltage",
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
                        "name": "ChargePowerWhenRSOCGreaterThan60",
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
                        "name": "ChargePowerWhenRSOCLessThanOrEqualTo60",
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
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DwellData",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "DwellTimeInMinutes",
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
                        "name": "DwellRSOCLevelAverage",
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
                        "name": "DwellTemperaturInKelvin",
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
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DischargeData",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "DischargeTimeInMinutes",
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
                        "name": "DischargeTemperaturInKelvin",
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
                        "name": "DischargeCurrent",
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
                        "name": "DischargeVoltage",
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
                        "name": "DischargePower",
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
                        "name": "DischargeDepthSessionGt80",
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
                        "name": "DischargeDepthSession60to80",
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
                        "name": "DischargeDepthSession40to60",
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
                        "name": "DischargeDepthSession20to40",
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
                        "name": "DischargeDepthSession10to20",
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
                        "name": "DischargeDepthSession5to10",
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
                        "name": "DischargeDepthSessionLe5",
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
                        "name": "DischargeStartLevelSessionGt94",
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
                        "name": "DischargeStartLevelSessionGt70to94",
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
                        "name": "DischargeStartLevelSessionGt50to70",
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
                        "name": "DischargeStartLevelSessionGt30to50",
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
                        "name": "DischargeStartLevelSessionGt10to30",
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
                        "name": "DischargeStartLevelSessionLe10",
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
                        "name": "DischargeEndLevelSession10to15",
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
                        "name": "DischargeEndLevelSession5to10",
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
                        "name": "DischargeEndLevelSessionLt5",
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
                        "name": "DischargePowerPercentageGe60",
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
                        "name": "DischargePowerPercentage50to60",
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
                        "name": "DischargePowerPercentage40to50",
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
                        "name": "DischargePowerPercentage30to40",
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
                        "name": "DischargePowerPercentage25to30",
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
                        "name": "DischargePowerPercentage20to25",
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
                        "name": "DischargePowerPercentage15to20",
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
                        "name": "DischargePowerPercentage10to15",
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
                        "name": "DischargePowerPercentage5to10",
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
                        "name": "DischargePowerPercentageLt5",
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
    ],
}

current_schema = {
    "type": "struct",
    "fields": [
        {
            "name": "ChargeData",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "ChargeTimeInMinutes",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "FullChargeSessionNumber",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "PartialChargeSessionNumber",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "ChargeTemperaturInKelvin",
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
                        "name": "ChargeCurrent",
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
                        "name": "ChargeVoltage",
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
                        "name": "ChargePowerWhenRSOCGreaterThan60",
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
                        "name": "ChargePowerWhenRSOCLessThanOrEqualTo60",
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
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DwellData",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "DwellTimeInMinutes",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DwellRSOCLevelAverage",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DwellTemperaturInKelvin",
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
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "DischargeData",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "name": "DischargeTimeInMinutes",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeTemperaturInKelvin",
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
                        "name": "DischargeCurrent",
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
                        "name": "DischargeVoltage",
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
                        "name": "DischargePower",
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
                        "name": "DischargeDepthSessionGt80",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeDepthSession60to80",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeDepthSession40to60",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeDepthSession20to40",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeDepthSession10to20",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeDepthSession5to10",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeDepthSessionLe5",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeStartLevelSessionGt94",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeStartLevelSessionGt70to94",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeStartLevelSessionGt50to70",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeStartLevelSessionGt30to50",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeStartLevelSessionGt10to30",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeStartLevelSessionLe10",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeEndLevelSession10to15",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeEndLevelSession5to10",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargeEndLevelSessionLt5",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentageGe60",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentage50to60",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentage40to50",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentage30to40",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentage25to30",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentage20to25",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentage15to20",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentage10to15",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentage5to10",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                    {
                        "name": "DischargePowerPercentageLt5",
                        "type": "float",
                        "nullable": True,
                        "metadata": {},
                    },
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {"name": "CycleCount", "type": "integer", "nullable": True, "metadata": {}},
    ],
}

table_schema = T.StructType.fromJson(current_schema)

def get_generator(rows, rate, is_stream, partitions) -> pyspark.sql.DataFrame:
  proto_file, message_name = ('BatteryAnalysis', 'BatteryAnalysis')

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
      'ChargeData',
      values=[None]
    )
    .withColumnSpec(
      'DwellData',
      values=[None]
    )
    .withColumnSpec(
      'DischargeData',
      values=[None]
    )
    .withColumnSpec(
      'CycleCount',
      minValue=0,
      maxValue=1000
    )
  )

  proto_df = dataspec.build(
    withStreaming=is_stream,
    options={'rowsPerSecond': rate}
  )

  # ChargeTemperaturInKelvin: struct (nullable = true)
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
  def generate_ChargeTemperaturInKelvin() -> pyspark.sql.Row:
    maximum = random.randint(280, 400)
    minimum = random.randint(280, maximum)
    average = random.randint(minimum, maximum)
    sd = random.random() * 10
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  #  |    |-- ChargeCurrent: struct (nullable = true)
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
  def generate_ChargeCurrent() -> pyspark.sql.Row:
    maximum = random.random() * 0.2 + 1
    minimum = random.random() * maximum
    average = random.random() * (maximum - minimum)
    sd = random.random()
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  def generate_ChargeVoltage() -> pyspark.sql.Row:
    maximum = random.random()
    minimum = random.random() * maximum
    average = random.random() * (maximum - minimum)
    sd = random.random() / 10
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average + 16.5 if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum + 16.5 if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum + 16.5 if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  def generate_ChargePowerWhenRSOCGreaterThan60() -> pyspark.sql.Row:
    maximum = random.randint(0, 20)
    minimum = random.randint(0, maximum)
    average = random.randint(minimum, maximum)
    sd = random.random() * 5
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  def generate_ChargePowerWhenRSOCLessThanOrEqualTo60() -> pyspark.sql.Row:
    maximum = random.randint(0, 20)
    minimum = random.randint(0, maximum)
    average = random.randint(minimum, maximum)
    sd = random.random() * 5
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  def generate_DwellTemperaturInKelvin() -> pyspark.sql.Row:
    maximum = random.randint(280, 400)
    minimum = random.randint(280, maximum)
    average = random.randint(minimum, maximum)
    sd = random.random() * 10
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  def generate_DischargeTemperaturInKelvin() -> pyspark.sql.Row:
    maximum = 0
    minimum = 0
    average = 0
    sd = 0
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  def generate_DischargeCurrent() -> pyspark.sql.Row:
    maximum = 0
    minimum = 0
    average = 0
    sd = 0
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  def generate_DischargeVoltage() -> pyspark.sql.Row:
    maximum = 0
    minimum = 0
    average = 0
    sd = 0
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  def generate_DischargePower() -> pyspark.sql.Row:
    maximum = 0
    minimum = 0
    average = 0
    sd = 0
    latest = None
    mode = None

    return pyspark.sql.Row(
      Average = pyspark.sql.Row(value=average if average != 0 else None),
      Maximum = pyspark.sql.Row(value=maximum if maximum != 0 else None),
      Minimum = pyspark.sql.Row(value=minimum if minimum != 0 else None),
      StandardDeviation = pyspark.sql.Row(value=sd if sd != 0 else None),
      Latest = pyspark.sql.Row(value=latest),
      Mode = [pyspark.sql.Row(value=mode)]
    )


  # ChargeData: struct (nullable = true)
  #  |-- ChargeTimeInMinutes: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- FullChargeSessionNumber: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- PartialChargeSessionNumber: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- ChargeTemperaturInKelvin: struct (nullable = true)
  #  |-- ChargeCurrent: struct (nullable = true)
  #  |-- ChargeVoltage: struct (nullable = true)
  #  |-- ChargePowerWhenRSOCGreaterThan60: struct (nullable = true)
  #  |-- ChargePowerWhenRSOCLessThanOrEqualTo60: struct (nullable = true)
  def generate_ChargeData() -> pyspark.sql.Row:
    charge_time = random.random() * 120
    full_charge = random.randint(0, 1)
    partial_charge = random.randint(0, 1)

    return pyspark.sql.Row(
      ChargeTimeInMinutes = pyspark.sql.Row(value=charge_time if charge_time != 0 else None),
      FullChargeSessionNumber = pyspark.sql.Row(value=full_charge if full_charge != 0 else None),
      PartialChargeSessionNumber = pyspark.sql.Row(value=partial_charge if partial_charge != 0 else None),
      ChargeTemperaturInKelvin = generate_ChargeTemperaturInKelvin(),
      ChargeCurrent = generate_ChargeCurrent(),
      ChargeVoltage = generate_ChargeVoltage(),
      ChargePowerWhenRSOCGreaterThan60 = generate_ChargePowerWhenRSOCGreaterThan60(),
      ChargePowerWhenRSOCLessThanOrEqualTo60 = generate_ChargePowerWhenRSOCLessThanOrEqualTo60()
    )


  # DwellData: struct (nullable = true)
  #  |-- DwellTimeInMinutes: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DwellRSOCLevelAverage: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DwellTemperaturInKelvin: struct (nullable = true)
  def generate_DwellData() -> pyspark.sql.Row:
    dw_time = random.random() * 600
    dwrsoc = random.randint(0, 100)

    return pyspark.sql.Row(
      DwellTimeInMinutes = pyspark.sql.Row(value=dw_time if dw_time != 0 else None),
      DwellRSOCLevelAverage = pyspark.sql.Row(value=dwrsoc if dwrsoc != 0 else None),
      DwellTemperaturInKelvin = generate_DwellTemperaturInKelvin()
    )


  # DischargeData: struct (nullable = true)
  #  |-- DischargeTimeInMinutes: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargeTemperaturInKelvin: struct (nullable = true)
  #  |-- DischargeCurrent: struct (nullable = true)
  #  |-- DischargeVoltage: struct (nullable = true)
  #  |-- DischargePower: struct (nullable = true)
  #  |-- DischargeDepthSessionGt80: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeDepthSession60to80: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeDepthSession40to60: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeDepthSession20to40: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeDepthSession10to20: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeDepthSession5to10: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeDepthSessionLe5: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeStartLevelSessionGt94: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeStartLevelSessionGt70to94: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeStartLevelSessionGt50to70: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeStartLevelSessionGt30to50: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeStartLevelSessionGt10to30: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeStartLevelSessionLe10: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeEndLevelSession10to15: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeEndLevelSession5to10: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargeEndLevelSessionLt5: struct (nullable = true)
  #  |    |-- value: integer (nullable = true)
  #  |-- DischargePowerPercentageGe60: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentage50to60: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentage40to50: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentage30to40: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentage25to30: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentage20to25: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentage15to20: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentage10to15: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentage5to10: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  #  |-- DischargePowerPercentageLt5: struct (nullable = true)
  #  |    |-- value: float (nullable = true)
  def generate_DischargeData() -> pyspark.sql.Row:
    return pyspark.sql.Row(  
      DischargeTimeInMinutes             = pyspark.sql.Row(value=None),
      DischargeTemperaturInKelvin        = generate_DischargeTemperaturInKelvin(),
      DischargeCurrent                   = generate_DischargeCurrent(),
      DischargeVoltage                   = generate_DischargeVoltage(),
      DischargePower                     = generate_DischargePower(),
      DischargeDepthSessionGt80          = pyspark.sql.Row(value=None),
      DischargeDepthSession60to80        = pyspark.sql.Row(value=None),
      DischargeDepthSession40to60        = pyspark.sql.Row(value=None),
      DischargeDepthSession20to40        = pyspark.sql.Row(value=None),
      DischargeDepthSession10to20        = pyspark.sql.Row(value=None),
      DischargeDepthSession5to10         = pyspark.sql.Row(value=None),
      DischargeDepthSessionLe5           = pyspark.sql.Row(value=None),
      DischargeStartLevelSessionGt94     = pyspark.sql.Row(value=None),
      DischargeStartLevelSessionGt70to94 = pyspark.sql.Row(value=None),
      DischargeStartLevelSessionGt50to70 = pyspark.sql.Row(value=None),
      DischargeStartLevelSessionGt30to50 = pyspark.sql.Row(value=None),
      DischargeStartLevelSessionGt10to30 = pyspark.sql.Row(value=None),
      DischargeStartLevelSessionLe10     = pyspark.sql.Row(value=None),
      DischargeEndLevelSession10to15     = pyspark.sql.Row(value=None),
      DischargeEndLevelSession5to10      = pyspark.sql.Row(value=None),
      DischargeEndLevelSessionLt5        = pyspark.sql.Row(value=None),
      DischargePowerPercentageGe60       = pyspark.sql.Row(value=None),
      DischargePowerPercentage50to60     = pyspark.sql.Row(value=None),
      DischargePowerPercentage40to50     = pyspark.sql.Row(value=None),
      DischargePowerPercentage30to40     = pyspark.sql.Row(value=None),
      DischargePowerPercentage25to30     = pyspark.sql.Row(value=None),
      DischargePowerPercentage20to25     = pyspark.sql.Row(value=None),
      DischargePowerPercentage15to20     = pyspark.sql.Row(value=None),
      DischargePowerPercentage10to15     = pyspark.sql.Row(value=None),
      DischargePowerPercentage5to10      = pyspark.sql.Row(value=None),
      DischargePowerPercentageLt5        = pyspark.sql.Row(value=None)
    )


  # Nested fields need to be added:
  # ChargeData is struct
  # DwellData is struct
  # DischargeData is struct

  ChargeData_schema = T.StructType.fromJson(original_schema["fields"][0]['type'])
  DwellData_schema = T.StructType.fromJson(original_schema["fields"][1]['type'])
  DischargeData_schema = T.StructType.fromJson(original_schema["fields"][2]['type'])

  generate_ChargeData_udf = F.udf(generate_ChargeData, ChargeData_schema)
  generate_DwellData_udf = F.udf(generate_DwellData, DwellData_schema)
  generate_DischargeData_udf = F.udf(generate_DischargeData, DischargeData_schema)

  add_nest_field_df = (
    proto_df
    .withColumn(
      'ChargeData', 
      generate_ChargeData_udf()
    )
    .withColumn(
      'DwellData', 
      generate_DwellData_udf()
    )
    .withColumn(
      'DischargeData', 
      generate_DischargeData_udf()
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
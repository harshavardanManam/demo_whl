import pyspark.sql.functions as F
import pyspark.sql.types as T
import math
import pyspark
import argparse
import traceback


from protobuf_generators.message_generators import *
from proto_ingestion.utils.get_token import *
from proto_ingestion.utils.helpers import *
from pyspark.sql import SparkSession
from datetime import datetime


def parse_args(params=None) -> argparse.Namespace:
  """
    Parse commandline arguments

    Parameters
    ----------
    params : list
      pre-defined parameters
      default : None
  """
  default_params = {
    'deposit_sas': 'NA',
    'sp_credential_key': 'astra-dbradvsr-dev-scu-sp-client-secret',
    'app_id_key': 'astra-dbradvsr-dev-scu-sp-client-id',
    'directory_id_key': 'astra-dbradvsr-dev-scu-sp-tenant-id',
    'scope': 'scu-devops-secret',
    'deposit_account': 'astradbradvsrdevscusa',
    'deposit_container': 'data',
    'interval': '30 seconds',
    'multiplier': 5,
    'num_cores': 64,
    'debug': 'True',
    'env': 'DEV'
  }

  parser = argparse.ArgumentParser(
    prog='proto_ingestion',
    description='Protobuf raw table task.'
  )

  parser.add_argument('--deposit_sas',  
    default=default_params['deposit_sas'],
    help='SAS token name to access deposit storage account.')                 
  parser.add_argument('--deposit_account',  
    default=default_params['deposit_account'],
    help='Storage account for storing protobuf raw table.')
  parser.add_argument('--deposit_container',  
    default=default_params['deposit_container'],
    help='Storage container for storing protobuf raw table.')
  parser.add_argument('--scope',  
    default=default_params['scope'],
    help='Scope name stores the secrets.')                    
  parser.add_argument('--sp_credential_key',  
    default=default_params['sp_credential_key'],
    help='Service principal for accessing storage account.')
  parser.add_argument('--app_id_key',  
    default=default_params['app_id_key'],
    help='Application id for service principal.')
  parser.add_argument('--directory_id_key',  
    default=default_params['directory_id_key'],
    help='Directory id for service principal.')
  parser.add_argument('--interval',  
    default=default_params['interval'], 
    help='Trigger interval.')
  parser.add_argument('--multiplier', 
    type=int,
    default=default_params['multiplier'], 
    help='Rate multiplier.')
  parser.add_argument('--num_cores',
    type=int,
    default=default_params['num_cores'], 
    help='Parallelism.')
  parser.add_argument('--debug',  
    default=default_params['debug'], 
    help='Enable debug mode.')
  parser.add_argument('--env', 
    default=default_params['env'], 
    help='Current job environment DEV/STAGING/PROD.')

  return parser.parse_known_args()[0] if not params else parser.parse_known_args(params)[0]


def get_content_size(datatype_id):
  if not datatype_id:
    return 0

  proto_size_map = {
    '81423c33-8358-4ffd-8771-568094b919bf': 867,
    '4136acd9-b69f-4b6d-96e7-233932755b33': 1033,
    '11036c82-3669-48c3-80ce-00aad3e121b4': 877,
    '9f45e5f6-4599-4494-a0f4-8a710d29ac59': 546,
    'c9b162e7-c8e7-4e53-b338-295f6afad85d': 1068,
    '70350172-f987-4e0b-9b9b-3192d8fdf17d': 741,
    '10466b48-557f-4966-a697-2247d2f3cb4b': 844,
    'c35cd409-7706-4204-8323-17452b9b86dd': 2680,
    'b7e11d31-cdcd-4842-beb0-c2d7c897b9da': 592,
    'eefec92c-eed2-4e7a-9711-c39f90d24ad6': 684,
    '652c6965-f001-4cf4-b980-90f42a502158': 513,
    'b9bdc4e0-3d19-4cd0-941c-b51332d4a87f': 1296
  }
  
  return proto_size_map[datatype_id]


def batch_partitioner(key, partition_list):
  return partition_list.index(key)


def generate_file_split(epoch_id, part_id, partition_iter):
  threshold = 1126400
  file_id = part_id * 10000
  cur_size = 0

  for row in partition_iter:
    if cur_size < threshold:
      cur_size += row['estimated_size']
      yield (
        file_id, 
        pyspark.sql.Row(
          Content=row.Content,
          DatatypeId=row.DatatypeId,
          StatusName=row.StatusName,
          # estimated_size=row.estimated_size,
          # file_id=file_id
        )
      )

    else:
      print(f'[Batch {epoch_id} - {part_id}] Generating split {file_id} with size: {cur_size}')
      cur_size = 0
      file_id += 1

  print(f'[Batch {epoch_id} - {part_id}] Generating split {file_id} with size: {cur_size}')


def foreach_batch_function(df, epoch_id, dep_base):
  print(f'[Batch {epoch_id}] Start to generate file spilt.')
  file_rdd = (
    df.rdd
    .mapPartitionsWithIndex(
      lambda part_id, partition_iter: generate_file_split(epoch_id, part_id, partition_iter)
    )
  )

  partition_list = list(
    file_rdd
    .countByKey()
    .keys()
  )

  num_partitions = len(partition_list)

  print(f'[Batch {epoch_id}] Detected {num_partitions} file splits in this batch.')
  if num_partitions == 0:
    return

  element_schema = T.StructType([
    df.schema['Content'],
    df.schema['DatatypeId'],
    df.schema['StatusName']
  ])
  new_schema = T.ArrayType(element_schema, True)

  final_df = (
    file_rdd
    .partitionBy(num_partitions, lambda x: batch_partitioner(x, partition_list))
    .mapPartitions(lambda part_iter: [[record[1] for record in part_iter]])
    .toDF(new_schema)
    .withColumn('value', F.to_json(F.col('value')))
  )

  try:
    current_time = datetime.utcnow()
    y, m, d, h, mm = current_time.year, current_time.month, current_time.day, current_time.hour, current_time.minute

    ( 
      final_df
      .write
      .format('text')
      .option('path', f'{dep_base}/table/protobuf_generator/y={y}/m={m}/d={d}/h={h}/id={mm}_{epoch_id}')
      .save()
    )
  except pyspark.sql.utils.AnalysisException:
    print(traceback.format_exc())
    print(f'Skipping batch {epoch_id}.')


def main():
  args  = parse_args()

  # proto_envelop_size = 135
  sp_credential_key = args.sp_credential_key
  app_id_key = args.app_id_key
  directory_id_key = args.directory_id_key
  scope = args.scope
  deposit_account_name = args.deposit_account
  deposit_container_name = args.deposit_container
  interval = args.interval
  multiplier = args.multiplier
  num_cores = args.num_cores

  dep_base                = f'abfss://{deposit_container_name}@{deposit_account_name}.dfs.core.windows.net'         # construct base path for data generation

  spark = ( 
    SparkSession
    .builder
    .appName("Protobuf generator")
    .getOrCreate()
  )

  dbutils = get_dbutils(spark)

  if sp_credential_key != 'NA':
    app_id        = dbutils.secrets.get(scope, app_id_key)
    sp_credential = dbutils.secrets.get(scope, sp_credential_key)
    directory_id  = dbutils.secrets.get(scope, directory_id_key)

    set_sp_token(deposit_account_name, sp_credential, app_id, directory_id)

  datatype_list = [
    '11036c82-3669-48c3-80ce-00aad3e121b4',
    '4136acd9-b69f-4b6d-96e7-233932755b33',
    '81423c33-8358-4ffd-8771-568094b919bf',
    'c35cd409-7706-4204-8323-17452b9b86dd',
    'b9bdc4e0-3d19-4cd0-941c-b51332d4a87f',
    'c9b162e7-c8e7-4e53-b338-295f6afad85d',
    'eefec92c-eed2-4e7a-9711-c39f90d24ad6',
    '70350172-f987-4e0b-9b9b-3192d8fdf17d',
    '9f45e5f6-4599-4494-a0f4-8a710d29ac59',
    'b7e11d31-cdcd-4842-beb0-c2d7c897b9da',
    '652c6965-f001-4cf4-b980-90f42a502158',
    '10466b48-557f-4966-a697-2247d2f3cb4b',
  ]

  proto_rate = {
    '11036c82-3669-48c3-80ce-00aad3e121b4': math.ceil(4 * multiplier), 
    '4136acd9-b69f-4b6d-96e7-233932755b33': math.ceil(8 * multiplier), 
    '81423c33-8358-4ffd-8771-568094b919bf': math.ceil(12 * multiplier), 
    'c35cd409-7706-4204-8323-17452b9b86dd': math.ceil(26 * multiplier), 
    'b9bdc4e0-3d19-4cd0-941c-b51332d4a87f': math.ceil(1003 * multiplier), 
    'eefec92c-eed2-4e7a-9711-c39f90d24ad6': math.ceil(1003 * multiplier), 
    '9f45e5f6-4599-4494-a0f4-8a710d29ac59': math.ceil(1003 * multiplier), 
    'b7e11d31-cdcd-4842-beb0-c2d7c897b9da': math.ceil(1003 * multiplier), 
    '652c6965-f001-4cf4-b980-90f42a502158': math.ceil(1003 * multiplier), 
    'c9b162e7-c8e7-4e53-b338-295f6afad85d': math.ceil(1003 * multiplier), 
    '70350172-f987-4e0b-9b9b-3192d8fdf17d': math.ceil(1003 * multiplier), 
    '10466b48-557f-4966-a697-2247d2f3cb4b': math.ceil(2008 * multiplier), 
  }

  proto_module = {
    '11036c82-3669-48c3-80ce-00aad3e121b4': 'BatteryAnalysis_BatteryAnalysis', 
    '9f45e5f6-4599-4494-a0f4-8a710d29ac59': 'BatteryDynamicData_BatteryDynamicData', 
    'eefec92c-eed2-4e7a-9711-c39f90d24ad6': 'BatteryStaticData_BatteryStaticData', 
    '4136acd9-b69f-4b6d-96e7-233932755b33': 'CpuAnalysis_CpuAnalysis', 
    '10466b48-557f-4966-a697-2247d2f3cb4b': 'CpuDynamicData_CpuDynamicData', 
    'c9b162e7-c8e7-4e53-b338-295f6afad85d': 'CpuStaticInfo_CpuStaticInfo', 
    'c35cd409-7706-4204-8323-17452b9b86dd': 'EventLog_ApplicationCrashEvent', 
    '81423c33-8358-4ffd-8771-568094b919bf': 'EventLog_DiagnosticPerformanceEvent', 
    'b9bdc4e0-3d19-4cd0-941c-b51332d4a87f': 'OperatingSystemInfo_OperatingSystemInfo', 
    '652c6965-f001-4cf4-b980-90f42a502158': 'OSSystemPerf_OSSystemPerf', 
    '70350172-f987-4e0b-9b9b-3192d8fdf17d': 'SystemboardInfo_SystemboardInfo', 
    'b7e11d31-cdcd-4842-beb0-c2d7c897b9da': 'SystemInformation_SystemInformation', 
  }


  protobuf_generator_namespace = {}

  sum_rate = 0
  sum_partitions = 0 
  union_df = None
  num_object = 1000 * multiplier
  batch_multiplier = 10000 * multiplier

  # Generate protobuf message
  for datatype_id in datatype_list:
    module_name = proto_module[datatype_id]
    rate = proto_rate[datatype_id]
    partitions = math.ceil(rate/1000)

    protobuf_generator_namespace[f'{module_name}_df'] = (
      globals()[module_name].get_generator(
        rows=0,
        rate=rate,
        is_stream=True,
        partitions=partitions
      )
      .withColumn('generated_at', F.current_timestamp())
      .withColumn('datatype_id', F.lit(datatype_id))
      .withColumn('object_id', F.ceil(F.rand(42) * num_object).cast('long') + F.col('generated_at').cast('long') * batch_multiplier)
      .withWatermark('generated_at', '10 seconds')
    )
    
    if not union_df:
      union_df = protobuf_generator_namespace[f'{module_name}_df']
    else:
      union_df = union_df.union(protobuf_generator_namespace[f'{module_name}_df'])
    
    sum_rate += rate
    sum_partitions += partitions

  print(f'Total rate: {sum_rate}')
  print(f'Total partitions: {sum_partitions}')
  spark.conf.set('spark.sql.shuffle.partitions', num_cores * multiplier)

  # Generate data for content fields
  content_df = (
    union_df
    .withColumn(
      'content',
      F.struct(
        F.struct(
          F.lit(None).cast(T.StringType()).alias('Value')
        ).alias('AlertId'), 
        F.lit(None).cast(T.StringType()).alias('AlertScore'), 
        F.lit(None).cast(T.LongType()).alias('AlertType'), 
        F.lit(None).cast(T.ArrayType(T.StringType(), True)).alias('AnalysisDetails'), 
        F.struct(
          F.lit('f93e0fb4-d1f9-4078-886f-02ddfd66f965').cast(T.StringType()).alias('Value')
        ).alias('AnalysisId'), 
        F.lit(None).cast(T.StringType()).alias('AnalysisScore'), 
        F.lit(None).cast(T.LongType()).alias('AnalysisType'), 
        F.lit(None).cast(T.ArrayType(T.StringType(), True)).alias('CollectionDetails'), 
        F.struct(
          F.lit('81abfc5c-d558-4bbe-8df1-ca1738d69cc1').cast(T.StringType()).alias('Value')
        ).alias('CollectionId'), 
        F.current_timestamp().cast(T.StringType()).alias('FromTime'), 
        F.rand(0).cast(T.DoubleType()).alias('InputQuality'), 
        F.lit(None).cast(T.StringType()).alias('Interval'), 
        F.struct(
          F.when(
            F.col('datatype_id') == '10466b48-557f-4966-a697-2247d2f3cb4b', 
            F.lit('Cpu_total') 
          )
          .when(
            F.col('datatype_id') == '11036c82-3669-48c3-80ce-00aad3e121b4', 
            F.lit('Battery_LeftBayBattery') 
          )
          .when(
            F.col('datatype_id') == '4136acd9-b69f-4b6d-96e7-233932755b33', 
            F.lit('Cpu_0') 
          )
          .when(
            F.col('datatype_id') == '652c6965-f001-4cf4-b980-90f42a502158', 
            F.lit('OSSystemPerf') 
          )
          .when(
            F.col('datatype_id') == '70350172-f987-4e0b-9b9b-3192d8fdf17d', 
            F.lit('SystemBoard') 
          )
          .when(
            F.col('datatype_id') == '81423c33-8358-4ffd-8771-568094b919bf', 
            F.lit('DiagnosticPerformanceEvent_7') 
          )
          .when(
            F.col('datatype_id') == '9f45e5f6-4599-4494-a0f4-8a710d29ac59', 
            F.lit('Battery_LeftBayBattery') 
          )
          .when(
            F.col('datatype_id') == 'b7e11d31-cdcd-4842-beb0-c2d7c897b9da', 
            F.lit('System') 
          )
          .when(
            F.col('datatype_id') == 'b9bdc4e0-3d19-4cd0-941c-b51332d4a87f', 
            F.lit('OperatingSystem') 
          )
          .when(
            F.col('datatype_id') == 'c35cd409-7706-4204-8323-17452b9b86dd', 
            F.lit('ApplicationCrashEvent_3') 
          )
          .when(
            F.col('datatype_id') == 'c9b162e7-c8e7-4e53-b338-295f6afad85d', 
            F.lit('Cpu_0') 
          )
          .when(
            F.col('datatype_id') == 'eefec92c-eed2-4e7a-9711-c39f90d24ad6', 
            F.lit('Battery_LeftBayBattery') 
          ).alias('Value')
        ).alias('ItemId'), 
        F.when(
          F.col('datatype_id') == '10466b48-557f-4966-a697-2247d2f3cb4b', 
          F.lit('84f09267-20fc-4db2-9423-b47f13676b39') 
        )
        .when(
          F.col('datatype_id') == '11036c82-3669-48c3-80ce-00aad3e121b4', 
          F.lit('7402a473-84a3-4fb1-b369-0eddcb0a2a97') 
        )
        .when(
          F.col('datatype_id') == '4136acd9-b69f-4b6d-96e7-233932755b33', 
          F.lit('942c5742-0c84-48d0-8d0a-4583d6b04015') 
        )
        .when(
          F.col('datatype_id') == '652c6965-f001-4cf4-b980-90f42a502158', 
          F.lit('84f09267-20fc-4db2-9423-b47f13676b39') 
        )
        .when(
          F.col('datatype_id') == '70350172-f987-4e0b-9b9b-3192d8fdf17d', 
          F.lit('396c7f51-56c3-4e0e-9915-1ed60a644b60') 
        )
        .when(
          F.col('datatype_id') == '81423c33-8358-4ffd-8771-568094b919bf', 
          F.lit('feba3e40-957a-4852-8d28-03220601e1fe') 
        )
        .when(
          F.col('datatype_id') == '9f45e5f6-4599-4494-a0f4-8a710d29ac59', 
          F.lit('27b7649d-9c7b-4a7c-b818-5a8dd81c0296') 
        )
        .when(
          F.col('datatype_id') == 'b7e11d31-cdcd-4842-beb0-c2d7c897b9da', 
          F.lit('396c7f51-56c3-4e0e-9915-1ed60a644b60') 
        )
        .when(
          F.col('datatype_id') == 'b9bdc4e0-3d19-4cd0-941c-b51332d4a87f', 
          F.lit('396c7f51-56c3-4e0e-9915-1ed60a644b60') 
        )
        .when(
          F.col('datatype_id') == 'c35cd409-7706-4204-8323-17452b9b86dd', 
          F.lit('feba3e40-957a-4852-8d28-03220601e1fe') 
        )
        .when(
          F.col('datatype_id') == 'c9b162e7-c8e7-4e53-b338-295f6afad85d', 
          F.lit('84f09267-20fc-4db2-9423-b47f13676b39') 
        )
        .when(
          F.col('datatype_id') == 'eefec92c-eed2-4e7a-9711-c39f90d24ad6', 
          F.lit('27b7649d-9c7b-4a7c-b818-5a8dd81c0296') 
        ).cast(T.StringType()).alias('PluginId'), 
        F.lit('2.0.0.5933').cast(T.StringType()).alias('PluginVersion'), 
        F.base64(F.col('serialized_blob')).alias('SerializedBlob'), 
        F.current_timestamp().cast(T.StringType()).alias('Timestamp'), 
        F.date_add(F.current_timestamp(), 1).cast(T.StringType()).alias('ToTime'), 
      )
    )
  )

  # Generate content object with random number of elements
  nested_content_df = (
    content_df
    .withColumn('event_time', F.current_timestamp())
    .withWatermark('event_time', '10 seconds')
    .groupBy(
      F.session_window(F.col('event_time'), '1 seconds'),
      'datatype_id', 
      'object_id'
    )
    .agg(F.collect_list('content').alias('Content'))
    .withColumn(
      'StatusName', 
      F.when(
        F.rand(0) > 0.0001, 
        F.lit('Success')
      )
      .when(
        F.rand(0) > 0.5,
        F.lit('Failure')
      ).otherwise(F.lit(None).cast(T.StringType()))
    )
  )

  get_content_size_udf = F.udf(get_content_size, T.IntegerType())

  # Estimate content size for each record with udf
  json_df = (
    nested_content_df
    .withColumn(
      'DatatypeId', 
      F.when(
        F.col('StatusName').isNotNull(),
        F.struct(
          F.col('datatype_id').alias('Value')
        )
      )
      .otherwise(
        F.lit(None)
        .cast(T.StructType([T.StructField('Value', T.StringType(), True)]))
      )
    )
    .withColumn(
      'Content',
      F.when(
        F.col('StatusName') == 'Failure',
        F.lit(None).cast(nested_content_df.schema['Content'].dataType)
      )
      .when(
        F.rand(0) <= 0.0001,
        F.lit(None)
      ).otherwise(F.col('Content'))
    )
    .withColumn(
      'estimated_size',
      get_content_size_udf(F.col('datatype_id')) * F.size('Content')
    )
    .select('Content', 'DatatypeId', 'StatusName', 'estimated_size')
  )

  # Removing previous checkpoint
  dbutils.fs.rm(f'{dep_base}/checkpoint/protobuf_generator', True)

  # Write to adls with foreachbatch
  ( 
    json_df
    .writeStream
    .foreachBatch(lambda df, epoch_id: foreach_batch_function(df, epoch_id, dep_base))
    .option('checkpointLocation', f'{dep_base}/checkpoint/protobuf_generator')
    .trigger(processingTime=interval)
    .start()
  )
import pyspark
import pyspark.sql.types as T
import pyspark.sql.functions as F

def df_to_col(df: pyspark.sql.DataFrame, cols: list, col_name: str) -> pyspark.sql.DataFrame:
  struct_fields = []
  other_fields = []

  for field in df.columns:
    if field in cols: 
      struct_fields.append(field)

    else: 
      other_fields.append(field)

  return (
    df
    .withColumn(col_name, F.struct(struct_fields))
    .select(other_fields + [col_name])
  )


def set_nest_col(
  df: pyspark.sql.DataFrame, 
  schema: dict, 
) -> pyspark.sql.DataFrame:
  """
    Flatten {value: data} contained in the schema and update the 
    dataframe using withColumn and withField operation.
    
    {value: data} included in array type field will not be handled
    by this function.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
      dataframe to be updated
    schema : dict
      jsonValue schema of the dataframe to be searched

    Return
    ----------
    df : pyspark.sql.DataFrame
      a new dataframe with column data nested
      in {value: data}
  """
  schema = T.StructType.fromJson(schema)
  struct_fields = schema.fields
  new_df = df

  if not struct_fields or len(struct_fields) < 1:
    return df

  for struct_field in struct_fields:
    field_type = struct_field.dataType

    if field_type.typeName() == 'struct':
      sub_fields = field_type.fields

      if len(sub_fields) == 1 and sub_fields[0].name == 'value' and sub_fields[0].dataType.typeName() != 'struct':
        print(f'df.withColumn({struct_field.name}, F.struct(F.col({struct_field.name}).alias(value)))')

        new_df = new_df.withColumn(
          struct_field.name, 
          F.struct(F.col(struct_field.name).alias('value'))
        )
      
  return new_df
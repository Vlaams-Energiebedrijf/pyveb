from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, DoubleType, DecimalType
from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd

# ensure consistency with transformations.add_metadata
meta_cols = [ ',\n"META_file_name" VARCHAR','"META_partition_date" TIMESTAMP','"META_processing_date_utc" TIMESTAMP','"META_loading_date_utc" TIMESTAMP default sysdate']

spark_meta_cols = {
    'META_file_name': StringType(),
    'META_partition_date': TimestampType(),
    'META_processing_date_utc': TimestampType()
}

spark_to_rs_meta_cols = [',\n"META_loading_date_utc" TIMESTAMP default sysdate']

mapping_sqlserver_redshift = {
    'smallint': 'BIGINT',
    'bigint': 'BIGINT',
    'bigint IDENTITY':'BIGINT',
    'bit':'BOOLEAN',
    'datetime':'TIMESTAMP',
    'datetime2':'TIMESTAMP',
    'decimal': 'NUMERIC',
    'int':'BIGINT',
    'numeric':'NUMERIC',
    'nvarchar': 'VARCHAR',
    # in cogenius, version is captured as timestamp which we arbitrarily translate to varchar
    'timestamp': 'VARCHAR(256)',
    'varchar': 'VARCHAR',
    'uniqueidentifier': 'VARCHAR(256)'
}

mapping_pandas_redshift = {
    'int64': 'BIGINT',
    'float64': 'DOUBLE PRECISION',
    'object': 'VARCHAR(MAX)'
}

mapping_pyspark_redshift = {
    'integer': 'BIGINT',
    'string': 'VARCHAR(MAX)',
    'double': 'DOUBLE PRECISION',
    'timestamp': 'TIMESTAMP',
    'boolean': 'BOOLEAN'
}

mapping_pandas_pyspark = {
    'int64': IntegerType(),
    'float64': DoubleType(),
    'object': StringType(),
    'bool': BooleanType(),
    'datetime64': TimestampType() 
}

mapping_sqlserver_pyspark = {
    'smallint': IntegerType(),
    'bigint': IntegerType(),
    'bigint IDENTITY': IntegerType(),
    'int': IntegerType(),
    'bit': BooleanType(),
    'datetime':TimestampType(),
    'datetime2': TimestampType(),
    'decimal': DecimalType(),
    'numeric': DecimalType(),
    'nvarchar': StringType(),
    # in cogenius, version is captured as timestamp which we arbitrarily translate to varchar
    'timestamp': StringType(),
    'varchar': StringType(),
    'uniqueidentifier': StringType()
}

def df_to_pyspark(df:pd.DataFrame):
    cols = df.columns
    dtypes = df.dtypes
    target_cols = []
    d = dict(zip(cols, dtypes))
    for key, val in d.items():
        key = key.strip().replace(' ', '_').replace('(','').replace(')','').replace(',','').replace(';','').replace('{','').replace('}','').replace('\n','').replace('\t','').replace('=','')
        target_type = mapping_pandas_pyspark[val.name]
        line = StructField(key, target_type, True)
        target_cols.append(line)
    for key, val in spark_meta_cols.items():
        line = StructField(key, val, True)
        target_cols.append(line)
    pyspark_schema = StructType(target_cols)
    return pyspark_schema

def sql_ddl_to_pyspark(sql_ddl):
    from simple_ddl_parser import DDLParser
    result = DDLParser(sql_ddl).run(output_mode='mssql')
    columns = result[0]['columns']
    # print(columns)
    cols = []
    dtypes = []
    sizes = []
    target_cols = []
    for col in columns:
        cols.append(col['name'])
        dtypes.append(col['type'])
        sizes.append(col['size'])
    zipped = zip(cols, dtypes, sizes)
    for item in zipped:
        key = item[0].strip().replace(' ', '_').replace('(','').replace(')','').replace(',','').replace(';','').replace('{','').replace('}','').replace('\n','').replace('\t','').replace('=','')
        target_type = mapping_sqlserver_pyspark[item[1]]
        if target_type == DecimalType():
            line = StructField(key, DecimalType(item[2][0], item[2][1]), True)
            print(line)
            target_cols.append(line)
        else:
            line = StructField(key, target_type, True)
            print(line)
            target_cols.append(line)
    for key, val in spark_meta_cols.items():
        line = StructField(key, val, True)
        print(line)
        target_cols.append(line)
    pyspark_schema = StructType(target_cols)
    return pyspark_schema
    
def pyspark_to_flyway_ddl(pyspark_schema, target_table, flyway_schema='ingest_schema', ):
    json_sch = pyspark_schema.jsonValue()
    target_cols = []
    for field in json_sch['fields']:
        target_name = field['name']
        target_type = mapping_pyspark_redshift[field['type']]
        line = f'"{target_name}" {target_type}'
        target_cols.append(line)

    joined_cols = ",\n".join(target_cols)
    joined_meta_cols = ",\n".join(spark_to_rs_meta_cols)
    final_cols = joined_cols+joined_meta_cols

    new_ddl = f"""CREATE TABLE IF NOT EXISTS ${{{flyway_schema}}}.{target_table} (
        {final_cols}
    )

    DISTSTYLE AUTO;

    ALTER TABLE ${{{flyway_schema}}}.{target_table} owner to ${{username}};
    """
    return new_ddl

def csv_to_flyway_ddl(cols, dtypes, target_table, flyway_schema='ingest_schema'):
    """
        ARGUMENTS
            cols: df.columns
            dtypes: df.dtypes
        RETURNS 
            flyway ddl 
    """
    target_cols = []
    d = dict(zip(cols, dtypes))
    for key, val in d.items():
        target_type = mapping_pandas_redshift[val.name]
        line = f'"{key}" {target_type}'
        target_cols.append(line)

    joined_cols = ",\n".join(target_cols)
    joined_meta_cols = ",\n".join(meta_cols)
    final_cols = joined_cols+joined_meta_cols

    flyway_ddl = f"""CREATE TABLE IF NOT EXISTS ${{{flyway_schema}}}.{target_table} (
        {final_cols}
    )

    DISTSTYLE AUTO;

    ALTER TABLE ${{{flyway_schema}}}.{target_table} owner to ${{username}};
    """
    return flyway_ddl

## function to convert cogenius sql server DDL to flyway compatible ddl
def sqlserver_to_flyway_ddl(sql_ddl:str, target_table:str, flyway_schema='ingest_schema'):
    """
        ARGUMENTS
            sql_ddl: generated sql ddl from cogenius DB 
            target_table: redshift table name without schema
    """


    from simple_ddl_parser import DDLParser
    result = DDLParser(sql_ddl).run(output_mode='mssql')
    columns = result[0]['columns']
    target_cols = []
    for col in columns:
        source_name = col['name']
        source_type = col['type']
        source_size = col['size']
        target_type = mapping_sqlserver_redshift[source_type]
        if target_type in ['VARCHAR']:
            if source_size:
                None
            else:
                source_size = 'MAX'
            target_type=f'{target_type}({source_size})'
        if target_type in ['NUMERIC']:
            target_type=f'{target_type}{source_size}'
        line = f'"{source_name}" {target_type}'
        target_cols.append(line)

    joined_cols = ",\n".join(target_cols)
    joined_meta_cols = ",\n".join(meta_cols)
    final_cols = joined_cols+joined_meta_cols

    new_ddl = f"""CREATE TABLE IF NOT EXISTS ${{{flyway_schema}}}.{target_table} (
        {final_cols}
    )

    DISTSTYLE AUTO;

    ALTER TABLE ${{{flyway_schema}}}.{target_table} owner to ${{username}};
    """
    return new_ddl

def write_flyway_ddl_to_file(flyway_ddl, target_table,local_path, flyway_schema='ingest'):
    from os import listdir
    from os.path import isfile, join
    flyway_dir = (f'{local_path}{flyway_schema}')
    files = [f for f in listdir(flyway_dir) if isfile(join(flyway_dir, f))]
    files.sort()
    version, seq = files[-1].split('__')[0].split('_')
    new_version = int(version.split('V')[-1])+1
    new_version = f'V{new_version:03}'
    new_name = f'{new_version}_1__create_{target_table}.sql'
    with open(f'{flyway_dir}/{new_name}', 'w') as file:
        file.write(flyway_ddl)
    file.close()

    ## /Users/pieterdepetter/GitHub-VEB/terra-etl/flyway/modules/redshift/



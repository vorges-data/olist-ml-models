# Databricks notebook source
# Biblioteca de Barra de progresso para execução de um script
%pip install tqdm

# COMMAND ----------

# Script Responsável por fazer a ingestão das features store
# safras analisadas para os sellers

import datetime
from tqdm import tqdm

# usar a query sql (importar a query vendas) para isso é necessário gerar a query sql para arquivo file para leitura no DataBricks
def import_query (path):
    with open(path, 'r') as open_file:
        return open_file.read()

# verificar se o database existe
def table_exists(database, table):
    count = (spark.sql(f"SHOW TABLES FROM {database}").filter(f"tableName = '{table}'").count())
    
    return count > 0    

# cria a interação das datas (range de datas)
def date_range(dt_start, dt_stop, period='daily'):
    # criar um lista de datas
    datetime_start = datetime.datetime.strptime(dt_start, "%Y-%m-%d")
    datetime_stop = datetime.datetime.strptime(dt_stop, "%Y-%m-%d")
    
    dates = []
    while datetime_start <= datetime_stop:
        dates.append(datetime_start.strftime('%Y-%m-%d'))
        datetime_start += datetime.timedelta(days=1)
    if period == 'daily':
        return dates
    elif period == 'monthly':
        return [i for i in dates if i.endswith('01')]


# dbutils -> Biblioteca do Databricks para não chumbar o valor e sim torna-lo dinamico
table = dbutils.widgets.get("table")
table_name = f"fs_vendedor_{table}"
database = "silver.analytics"
period = dbutils.widgets.get("period")

# importando a query
query = import_query(f'{table}.sql')

date_start = dbutils.widgets.get("date_start")
date_stop  = dbutils.widgets.get("date_stop")

dates = date_range(date_start, date_stop, period)

print(table_name, table_exists(database, table_name))
print(date_start, ' ~ ', date_stop)

# COMMAND ----------

if not table_exists(database, table_name):
    print('Criando a tabela...')
    df = spark.sql(query.format(date=dates.pop(0)))
    # Coalesce no spark traz o dado em um único núcleo
    (df.coalesce(1)
     
             .write.
             format('delta')
             .mode('overwrite')
             .option('overwriteSchema', 'true')
             .partitionBy('dtReference')
             .saveAsTable(f'{database}.{table_name}')
    )
    print('Ok!')
    

print('Realizando update')
for i in tqdm(dates):
    spark.sql(f"DELETE FROM {database}.{table_name} WHERE dtReference = '{i}'")
    ( spark.sql(query.format(date=i))
        .coalesce(1)
        .write
        .format('delta')
        .mode('append')
        .saveAsTable(f"{database}.{table_name}") )
print('Ok!')

# COMMAND ----------



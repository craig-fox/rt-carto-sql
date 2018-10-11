from __future__ import print_function
from carto.sql import SQLClient
from carto.auth import APIKeyAuthClient
from carto.exceptions import CartoException
from carto.datasets import DatasetManager
from carto.file_import import FileImportJobManager
import psycopg2
import json
import csv
import asyncio

def doSelect():
    try:
       script = 'select count(*) from vw_regions_entries'
       #script = 'select count(*) from vw_regions_entries_data_3'
       data = sql.send(script)
       print(data['rows'])
       print("All done")
    except CartoException as e:
        print("some error ocurred", e)

    #print(data['rows'])

def runScript(script):
    try:
        data = sql.send(script)
        print(data)
        print("All done")
        return data
    except CartoException as e:
        print("some error ocurred", e)

def createTable(script):
    print("the script is {script}".format(script=script))
    table_sql = script
    runScript(table_sql)
    
def insertIntoTable(script, source):
    max_carto_dbid = maxCartoId('vw_regions_entries')
    insert_sql = script.format(source=source, maxid=max_carto_dbid)
    print("the script is {script}".format(script=insert_sql))
    runScript(insert_sql)

def dropTable(table):
    delete_sql = "DROP TABLE {table}".format(table=table)
    print("the script is {script}".format(script=delete_sql))
    runScript(delete_sql)

def countRows(table):
    count_sql = "SELECT COUNT(*) FROM {table}".format(table=table)
    print("the script is {script}".format(script=count_sql))
    runScript(count_sql)

def maxCartoId(table):
    cartoid_sql = "SELECT MAX(cartodb_id) FROM {table}".format(table=table)
    print("the script is {script}".format(script=cartoid_sql))
    data = runScript(cartoid_sql)
    return data['rows'][0]['max']

def updateCartoId(table):
     max_carto_dbid = maxCartoId('vw_regions_entries')
     update_sql = "UPDATE {table} SET cartodb_id = (cartodb_id + {maxid}) WHERE 1=1".format(table=table, maxid=max_carto_dbid)
     print("the script is {script}".format(script=update_sql))
     runScript(update_sql)

def cartodbfy(table):
    carto_sql = "SELECT cdb_cartodbfytable('ryanjudd', '{table}')".format(table=table)
    print("the script is {script}".format(script=carto_sql))
    runScript(carto_sql)
    
config = None
tasks = None
with open('config.json') as json_data_file:
    config = json.load(json_data_file)
with open('./sql/vw_regions_entries_create.sql', 'r') as create_file:
    create_sql = create_file.read()
with open('./sql/vw_regions_entries_insert.sql', 'r') as insert_file:
    insert_sql = insert_file.read()

USERNAME=config['carto']['account']
USR_BASE_URL = config['carto']['url_base'].format(user=USERNAME)
auth_client = APIKeyAuthClient(api_key=config['carto']['api'], base_url=USR_BASE_URL)
print("Username is {user}, base url is {url}".format(user=USERNAME, url=USR_BASE_URL))

sql = SQLClient(auth_client)
doSelect()
#cartodbfy('vw_regions_entries')
#createTable(create_sql)
#insertIntoTable(insert_sql, 'vw_regions_entries_data_3')
#updateCartoId('vw_regions_entries_data_2')
#countRows('vw_regions_entries')
#dropTable('vw_regions_entries')
#clearTable('vw_regions_entries_exits_hourly')













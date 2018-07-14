#!/usr/local/sbin/python3.5

import argparse
from pymongo import MongoClient
import json
import ast
import time
import bson

from datetime import datetime
from datetime import timedelta
from datetime import date

def printDumps(host, banco, collection):

    client = MongoClient(host)
    db = client[banco]

    resultado = db[collection].find({}, {'execucoes': { '$slice': -1}})

    dicionario = {'data': []}

    for i in resultado:
        Id = i['_id']
        dicionario['data'].append({'{#ID}': Id})
        
    print(json.dumps(dicionario, indent=4))

def processaResultado(host, banco, collection, hostname):

    client = MongoClient(host)
    db = client[banco]

    resultado = db[collection].find({}, {'execucoes': { '$slice': -1}})
    
    tempo_agora = datetime.utcnow() #Horário do mongo NSV está em UTC

    for i in resultado:
        Id = i['_id']
        for y in i['execucoes']:
            sync = '{0:.2f}'.format((tempo_agora - y['fim']).total_seconds() / 60.0)
        print('Enviando: "{host}", "qtd[{id}]", {valor}'.format(host=hostname, id=Id, valor=sync))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Realiza queries no mongo.')
    parser.add_argument('host', help='Nome da Maquina que ira ser feita a conexao')
    parser.add_argument('banco', help='Nome do Banco (ex: mongo_prd_casasbahia)')
    parser.add_argument('collection', help='Nome da Collection')
    parser.add_argument('host_zabbix', help='Nome do host no Zabbix')

    args = parser.parse_args()

    printDumps(args.host, args.banco, args.collection)
    time.sleep(5) #Tempo para criação de descoberta e envio de resultados
    processaResultado(args.host, args.banco, args.collection, args.host_zabbix)
    

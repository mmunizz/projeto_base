import sys
from typing import List, Any, Union
import tweepy as tw
import pandas as pd
import pyodbc as pydb
import os
import json

def processa_twitts(cfg_json):
    conn = pydb.connect(
        'DRIVER={SQL Server};SERVER=' + cfg_json['server'] + ';DATABASE=' + cfg_json['database'] + ';uid=' + cfg_json[
            'username'] + ';pwd=' + cfg_json['password'])
    cursor = conn.cursor()
    autorizacao = tw.OAuthHandler(cfg_json['consumer_key'], cfg_json['consumer_secret'])
    autorizacao.set_access_token(cfg_json['access_token'], cfg_json['access_token_secret'])
    api = tw.API(autorizacao)
    cursor.execute("select top 1 LINHA from LINHA where mes=12 and ano='2019' order by qtd desc")
    linha = cursor.fetchone()
    query=""
    if linha is not None:
        query="AND " + linha[0]
    query = "BOTICÁRIO " + query
    pesquisa = api.search(q=query, lang='pt', count=50, tweet_mode="extended")
    for twitters in pesquisa:
        name_user = twitters.user.screen_name
        texto = twitters.full_text
        cursor.execute ("INSERT INTO bd.dbo.TWITTS (usuario, texto) values (?,?)", (name_user, texto))
        cursor.commit()
    conn.close()
def carrega_MarcaLinha(cfg_json):
    conn = pydb.connect(
        'DRIVER={SQL Server};SERVER=' + cfg_json['server'] + ';DATABASE=' + cfg_json['database'] + ';uid=' + cfg_json[
            'username'] + ';pwd=' + cfg_json['password'])
    cursor = conn.cursor()
    query = '''SELECT MARCA, LINHA, SUM(QTD_VENDA) AS QTD 
                from dbo.Base 
                group by Marca, linha 
            ORDER BY MARCA'''
    df1 = pd.read_sql(query, conn)
    cursor = conn.cursor()
    for row in df1.itertuples():
        cursor.execute('''
                    INSERT INTO bd.dbo.MARCA_LINHA (MARCA, LINHA, QTD)
                    VALUES (?,?,?)
                    ''',
                       row.MARCA,
                       row.LINHA,
                       row.QTD
                       )
        cursor.commit()
    conn.close()
def carrega_AnoMes(cfg_json):
    conn = pydb.connect(
        'DRIVER={SQL Server};SERVER=' + cfg_json['server'] + ';DATABASE=' + cfg_json['database'] + ';uid=' + cfg_json[
            'username'] + ';pwd=' + cfg_json['password'])
    query = '''SELECT  YEAR(CONVERT(DATETIME, DATA_VENDA, 103)) AS ANO, MONTH(CONVERT(DATETIME, DATA_VENDA, 103))AS MES, SUM(QTD_VENDA) AS QTD 
            from dbo.Base 
            group BY YEAR(CONVERT(DATETIME, DATA_VENDA, 103)), MONTH(CONVERT(DATETIME, DATA_VENDA, 103))'''
    df1 = pd.read_sql(query, conn)
    cursor = conn.cursor()
    for row in df1.itertuples():
        cursor.execute('''
                    INSERT INTO bd.dbo.ANO_MES (ANO, MES, QTD)
                    VALUES (?,?,?)
                    ''',
                       row.ANO,
                       row.MES,
                       row.QTD
                       )
        cursor.commit()
    conn.close()
def carrega_Linha(cfg_json):
    conn = pydb.connect(
        'DRIVER={SQL Server};SERVER=' + cfg_json['server'] + ';DATABASE=' + cfg_json['database'] + ';uid=' + cfg_json[
            'username'] + ';pwd=' + cfg_json['password'])
    query = '''SELECT  LINHA, YEAR(CONVERT(DATETIME, DATA_VENDA, 103)) ANO, MONTH(CONVERT(DATETIME, DATA_VENDA, 103)) MES, SUM(QTD_VENDA) AS QTD 
            from dbo.Base 
            group BY LINHA, YEAR(CONVERT(DATETIME, DATA_VENDA, 103)), MONTH(CONVERT(DATETIME, DATA_VENDA, 103)) '''
    df1 = pd.read_sql(query, conn)
    cursor = conn.cursor()
    for row in df1.itertuples():
        cursor.execute('''
                    INSERT INTO bd.dbo.LINHA (LINHA, ANO, MES, QTD)
                    VALUES (?,?,?,?)
                    ''',
                       row.LINHA,
                       row.ANO,
                       row.MES,
                       row.QTD
                       )
        cursor.commit()
    conn.close()
def carrega_Marca(cfg_json):
    conn = pydb.connect(
        'DRIVER={SQL Server};SERVER=' + cfg_json['server'] + ';DATABASE=' + cfg_json['database'] + ';uid=' + cfg_json[
            'username'] + ';pwd=' + cfg_json['password'])
    query = '''SELECT  MARCA, YEAR(CONVERT(DATETIME, DATA_VENDA, 103)) ANO, MONTH(CONVERT(DATETIME, DATA_VENDA, 103)) MES, SUM(QTD_VENDA) AS QTD from dbo.Base 
    group BY MARCA, YEAR(CONVERT(DATETIME, DATA_VENDA, 103)), MONTH(CONVERT(DATETIME, DATA_VENDA, 103)) '''
    df1 = pd.read_sql(query, conn)
    cursor = conn.cursor()
    for row in df1.itertuples():
        cursor.execute('''
                    INSERT INTO bd.dbo.MARCA (MARCA, ANO, MES, QTD)
                    VALUES (?,?,?,?)
                    ''',
                       row.MARCA,
                       row.ANO,
                       row.MES,
                       row.QTD
                       )
        cursor.commit()
    conn.close()
def processa_csv(data,cfg_json):
    conn = pydb.connect('DRIVER={SQL Server};SERVER=' + cfg_json['server'] + ';DATABASE=' + cfg_json['database'] + ';uid=' + cfg_json['username'] + ';pwd=' + cfg_json['password'])
    cursor = conn.cursor()
    df = pd.DataFrame(data, columns=['ID_MARCA', 'MARCA', 'ID_LINHA', 'LINHA', 'DATA_VENDA', 'QTD_VENDA'])
    for row in df.itertuples():
        cursor.execute('''
                            INSERT INTO bd.dbo.Base (ID_MARCA, MARCA, ID_LINHA, LINHA, DATA_VENDA, QTD_VENDA)
                            VALUES (?,?,?,?,?,?)
                            ''',
                       row.ID_MARCA,
                       row.MARCA,
                       row.ID_LINHA,
                       row.LINHA,
                       row.DATA_VENDA,
                       row.QTD_VENDA
                       )
        cursor.commit()
    conn.close()
def percorre_folder(cfg_json):
    for diretorio, subpastas, arquivos in os.walk(cfg_json['folder']):
        for arquivo in arquivos:
            data = pd.read_csv(os.path.join(diretorio, arquivo), sep=';')
            processa_csv(data, cfg_json)
def main():
    cfg_file = os.path.realpath(__file__).replace('.py', '.json')
    if os.path.exists(cfg_file):
        with open(cfg_file, encoding='utf8') as f:
            dados_raw = f.read()
        cfg_json = json.loads(dados_raw)
        percorre_folder(cfg_json)
        carrega_MarcaLinha(cfg_json)
        carrega_AnoMes(cfg_json)
        carrega_Linha(cfg_json)
        carrega_Marca(cfg_json)
        processa_twitts(cfg_json)

if __name__ == '__main__':
    main()


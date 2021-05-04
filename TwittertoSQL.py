import tweepy as tw
import sys
from typing import List, Any, Union

import pandas as pd
import pyodbc as pydb
import os
import json

consumer_key = 'oYsugLSJX4xvd18DnauvXoXsY'
consumer_secret = 'xpfqhfx5fqpnlnNk7hUVlV1o1oDPpniMwMM8pwktbeGE8ISi4m'
access_token = '133506101-0p5tzYj6hMGNQgnHP5NHD90irauBFnAOql2pxohO'
access_token_secret = 'GJXT94Fs1x8AXQv7BbfZNzN892KlLrpv0XFUQXdB9bx1U'

autorizacao = tw.OAuthHandler(consumer_key, consumer_secret)
autorizacao.set_access_token(access_token, access_token_secret)
api = tw.API(autorizacao)
query = "Boticário"
pesquisa = api.search(q=query,lang='pt',count=50,tweet_mode="extended")
resultado = []
for twitters in pesquisa:
  conn = pydb.connect(
    'DRIVER={SQL Server};SERVER=' + cfg_json['server'] + ';DATABASE=' + cfg_json['database'] + ';uid=' + cfg_json[
      'username'] + ';pwd=' + cfg_json['password'])
  cursor = conn.cursor()
  name_user = twitters.user.screen_name
  created_at = twitters.created_at
  resultado.append(twitters.full_text)




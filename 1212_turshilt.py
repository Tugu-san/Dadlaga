import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www2.1212.mn/tablesdata1212.aspx?ln=Mn&tbl_id=DT_NSO_0300_002V1&SOUM_select_all=1&SOUMSingleSelect=&YearY_select_all=1&YearYSingleSelect=&viewtype=table"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

table = soup.find('table')
df = pd.read_html(str(table))[0]
print(df.head())

df_transposed = df.transpose()
df_transposed = df_transposed.reset_index()
df_transposed = df_transposed.drop(1, axis=0).reset_index(drop=True)
df_transposed = df_transposed.drop(27, axis=1).reset_index(drop=True)
df_transposed.columns = df_transposed.iloc[0].reset_index(drop=True)
df_transposed = df_transposed[1:].reset_index(drop=True)
print(df_transposed.head())
from .create_dataframe import  extraer_links_todas_las_urls
from packages import  pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ruta=r'/home/sebastian/Documentos/programas/Webscrapping/all_links.xlsx'
df_urls=pd.read_excel(ruta,header=0,sheet_name='urls')
df_urls['url final']=df_urls['url final'].str.replace(" ", "", regex=False)
df_urls['url final'] = df_urls['url final'].str.strip()
df_urls.head()

df_prueba = df_urls[
    (df_urls['url final'] == 'https://ar.dewalt.global/productos/herramientas-electricas?page=num_pag') 
]
#df_prueba=df_urls[2:4]
print(df_prueba.head())
df_links_webpaginadas=extraer_links_todas_las_urls(df_prueba, max_workers=4)



"""
__init__.py

Este archivo convierte la carpeta 'webscraper' en un paquete de Python.
Permite inicializar el paquete y definir qué módulos y funciones estarán disponibles al importar 'webscraper'.

Aquí se importan y exponen las funciones principales de los submódulos, facilitando su acceso directo desde el paquete.
Por ejemplo, después de esto puedes hacer:
    from webscraper import extraer_links_url

El uso de __init__.py es necesario para que Python reconozca la carpeta como un paquete y para organizar los imports de manera centralizada.
"""
from .config import *
from .network import validar_url
from .link_web_paginda import obtener_links_web_paginada
from .extraction import extraer_links_url
from .create_dataframe import  extraer_links_todas_las_urls
from .link_web_scroll import verifica_scroll,scrapear_productos,HEADERS
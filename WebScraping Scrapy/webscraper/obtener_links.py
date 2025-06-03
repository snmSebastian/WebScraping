# Import scrapy
import scrapy
from scrapy.crawler import CrawlerProcess
import logging
import pprint
import random


'''
Concepto:
    * yield:  para entregar a Scrapy una nueva tarea (como visitar una página), permitiendo que el spider siga trabajando 
              y maneje muchas páginas al mismo tiempo, en vez de esperar a que termine una para empezar otra como pasaria con return.

    * callback: es una función que se llama cuando la solicitud se completa, permitiendo procesar la respuesta de esa solicitud.
                evita que el spider se detenga y espere a que se complete una solicitud antes de continuar con la siguiente.

    * self:permite llamar una función dentro de la misma clase

    *follow: es un método de Scrapy que se utiliza para seguir enlaces a otras páginas web.
            Sigue este enlace relativo o absoluto y, cuando descargues la página, llama a esta función”.
            Resuelve automáticamente URLs relativas (como /producto/123), usando la página actual como base.
    '''


url_base_pag = 'https://ar.dewalt.global/productos/herramientas-electricas?page=1'


user_agents = [
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15'
]




class Description_Sku_Spider(scrapy.Spider):
    name = "sku_spider"
    custom_settings = {
        'USER_AGENT': random.choice(user_agents),
        'DOWNLOAD_TIMEOUT': 30,
        "LOG_LEVEL": "CRITICAL"
    }

    def start_requests(self):
        '''
        Inicia el spider haciendo la primera solicitud a la página principal.
        arg: self (instancia del spider)
        yield: scrapy.Request que llamará a parse_front al recibir respuesta
        '''
        yield scrapy.Request(url=url_base_pag, callback=self.analyze_homepage)

    def analyze_homepage(self, response):
        '''
        Procesa la página principal, extrae enlaces de productos y crea solicitudes para cada uno.
        arg: response (respuesta HTML de la página principal)
        yield: response.follow para cada enlace, llamando a parse_pages
        '''
        # Todos los posibles nombres de las clases de los enlaces de productos
        class_options = ["product", "item", "card"]
        # Construye una expresión XPath para encontrar enlaces que contengan cualquiera de las clases especificadas
        xpath_expr = "//a[" + " or ".join([f"contains(@class, '{opt}')" for opt in class_options]) + "]/@href"
        # Extrae los hyperlinks de los productos usando la expresión XPath
        product_links = response.xpath(xpath_expr).getall()


        for url in product_links:
            yield response.follow(url=url, callback=self.analyze_product_detail)

    def analyze_product_detail(self, response):
        '''
        Procesa la página de cada producto, extrae título y descripción, y los guarda en un diccionario.
        arg: response (respuesta HTML de la página de producto)
        yield: None (solo guarda los datos en dc_dict)
        '''
        crs_title = response.xpath('//h1[contains(@class,"title")]/text()').get().strip() if crs_title else ""
        crs_descr = response.css('div[class*="description"]::text').get().strip() if crs_descr else ""
        
        
        dc_dict[crs_title] = crs_descr



dc_dict = dict()

process = CrawlerProcess(settings={"LOG_LEVEL": "CRITICAL"})
process.crawl(Description_Sku_Spider)
process.start()

# Imprime los resultados de forma legible
pprint.pprint(dc_dict)
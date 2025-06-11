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
    *meta: es un diccionario que se utiliza para pasar información adicional entre las solicitudes y las respuestas en Scrapy.
            se usara para rastrear el número de página actual y el número de errores de intentos por cada url base.
    
            '''


url_base = 'https://ar.dewalt.global/productos/herramientas-electricas?page=1'

import scrapy
from scrapy.crawler import CrawlerProcess
import logging
import pprint
import random
import pandas as pd
'''
            'link': response.url,
            'sku': response.xpath(self.xpath_sku).get(default='').strip(),
            'brand': response.xpath(self.xpath_brand).get(default='').strip(),
            'name_product': response.xpath(self.xpath_name_product).get(default='').strip(),
            'description_product': response.xpath(self.xpath_description_product).get(default='').strip(),
            'image_product': response.xpath(self.xpath_image_product).get(default='').strip(),
            'category_product': response.xpath(self.xpath_category_product).get(default='').strip(),
            'esp_tecnic_product': response.xpath(self.xpath_esp_tecnic_product).get(default='').strip(),
            'price_product': response.xpath(self.xpath_price_product).get(default='').strip(),
            'country': self.country,
            'name_page': self.name_page,
            'information': self.information,
            'url_base': self.url_base
            '''


class Description_Sku_Spider(scrapy.Spider):
    user_agents = [
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15'
    ]

    name = "sku_spider"
    custom_settings = {
        'USER_AGENT': random.choice(user_agents),
        'DOWNLOAD_TIMEOUT': 30,
        "LOG_LEVEL": "CRITICAL"
    }

    def __init__(self,country, name_page, information, secuencia, url_base,
                 xpath_link_sku, xpath_sku, xpath_brand,
                 xpath_name_product, xpath_description_product, xpath_image_product,
                 xpath_category_product, xpath_esp_tecnic_product, xpath_price_product,xpath_page,
                resultados=None,**kwargs):
        super().__init__(**kwargs)
        self.country = country
        self.name_page = name_page
        self.information = information
        try:
            self.secuencia = int(secuencia)
        except Exception:
            self.secuencia = 1
        self.url_base = url_base
        self.xpath_link_sku = xpath_link_sku
        self.xpath_sku = xpath_sku
        self.xpath_brand = xpath_brand
        self.xpath_name_product = xpath_name_product
        self.xpath_description_product = xpath_description_product
        self.xpath_image_product = xpath_image_product
        self.xpath_category_product = xpath_category_product
        self.xpath_esp_tecnic_product = xpath_esp_tecnic_product
        self.xpath_price_product = xpath_price_product
        self.xpath_page=xpath_page
        self.resultados = resultados if resultados is not None else []
        self.intentos_url = 0  # Contador de intentos para la url_base

    def start_requests(self):
        # Detecta si la url_base tiene paginación
        num_page = 0
        if 'num_pag' in self.url_base:  
            url_base_pag = self.url_base.replace('num_pag', str(num_page))
            yield scrapy.Request(
                url=url_base_pag,
                callback=self.analyze_homepage,
                errback=self.handle_error,
                meta={'num_page': num_page, 'errores_intentos': 0}
            )

        else:
            # Si no hay paginación, solo intenta una vez (o máximo dos si falla)
            yield scrapy.Request(
                url=self.url_base,
                callback=self.analyze_homepage,
                errback=self.handle_error,
                meta={'num_page': None, 'errores_intentos': 0}
            )

    
    def handle_error(self, failure):
        meta = failure.request.meta
        errores_intentos = meta.get('errores_intentos', 0) + 1

        if 'num_page' in meta and meta['num_page'] is not None and errores_intentos < 3:
        # Intenta con la siguiente página
            next_num_page = meta['num_page'] + 1
            next_url = self.url_base.replace('num_pag', str(next_num_page))
            self.logger.error(f"Error en la página {meta['num_page']}. Probando con la página {next_num_page} (Intento {errores_intentos})")
            yield scrapy.Request(
                url=next_url,
                callback=self.analyze_homepage,
                errback=self.handle_error,
                meta={'num_page': next_num_page, 'errores_intentos': errores_intentos}
            )
        elif 'num_page' in meta and meta['num_page'] is None and errores_intentos < 2:
            self.logger.error(f"Error en la url {self.url_base}. Probando  (Intento {errores_intentos})")
            yield scrapy.Request(
                url=self.url_base,
                callback=self.analyze_homepage,
                errback=self.handle_error,
                meta={'num_page': next_num_page, 'errores_intentos': errores_intentos}
            )
        else:
            self.logger.error(f"Demasiados intentos fallidos para la url {failure.request.url}. Deteniendo el spider.")
            raise scrapy.exceptions.CloseSpider('Demasiados intentos fallidos para la url_base')
    
    def analyze_homepage(self, response):
        print(f"Analizando la página {response.meta.get('num_page', 'N/A')} de {self.url_base}\n{'-'*80}")
        product_links = response.xpath(self.xpath_link_sku).getall()
        if not product_links:
            print(f"No se encontraron productos en la página {response.meta.get('num_page', 'N/A')} de {self.url_base}.\n{'-'*80}")
            return  # No hay más productos, detén la paginación

        for url in product_links:
            print(f"Enviando solicitud para el producto: {url}\n{'-'*80}")
            yield response.follow(url=url, callback=self.analyze_product_detail)

        # Si hay paginación, solicita la siguiente página
        if 'num_pag' in self.url_base and response.meta['num_page'] is not None:
            print(f'La pagina {self.url_base} tiene paginación, solicitando la siguiente página...\n{"-"*80}')
            print(f"self.secuencia = {self.secuencia} (tipo: {type(self.secuencia)})")
            num_page = response.meta['num_page'] + self.secuencia
            print(f'Número de página actual: {num_page}')
            next_url = self.url_base.replace('num_pag', str(num_page))
            print(f'URL de la siguiente página: {next_url}')
            print(f'solicitando la siguiente página: {next_url}\n{"-"*80}')
            yield scrapy.Request(
                url=next_url,
                callback=self.analyze_homepage,
                errback=self.handle_error,
                meta={'num_page': num_page}
            )

    def analyze_product_detail(self, response):
        print(f"Analizando los detalles del producto: {response.url}")
        # Extrae los datos del producto y los guarda en self.results
        def safe_xpath(xpath):
            try:
                value = response.xpath(xpath).get(default='').strip()
                return value if value else "no"
            except Exception:
                return "no"

        item = {
            'link': response.url,
            'sku': safe_xpath(self.xpath_sku),
            'brand': safe_xpath(self.xpath_brand),
            'name_product': safe_xpath(self.xpath_name_product),
            'description_product': safe_xpath(self.xpath_description_product),
            'image_product': safe_xpath(self.xpath_image_product),
            'category_product': safe_xpath(self.xpath_category_product),
            'esp_tecnic_product': safe_xpath(self.xpath_esp_tecnic_product),
            'price_product': safe_xpath(self.xpath_price_product),
            'country': self.country,
            'name_page': self.name_page,
            'information': self.information,
            'url_base': self.url_base
        }
        #para cada url_base se genera un diccionario con los datos de detalle de cada uno de los productos que se encuentran en cada una de 
        # las paginas de la url_base
        print(f"Datos del producto extraídos: {item}\n{'-'*80}")
        self.resultados.append(item)
        print(self.resultados)

    def closed(self, reason):
        # Guarda los resultados en el objeto crawler para poder acceder desde fuera
        print(f'Cerrando y guardando los resultados para la url_base: {self.url_base}')
        #self.crawler.stats.set_value('results', self.results)

def scrapear_productos(**kwargs):
    resultados = []
    kwargs['resultados'] = resultados  # Inicializa la lista de resultados en kwargs
    process = CrawlerProcess(settings={"LOG_LEVEL": "CRITICAL"})
    process.crawl(Description_Sku_Spider, **kwargs)
    process.start()
        
    if not resultados:
        return pd.DataFrame()
    return pd.DataFrame(resultados)


ruta=r'/home/sebastian/Documentos/programas/WebScraping/WebScraping Scrapy/all_links.xlsx'
df=pd.read_excel(ruta, sheet_name='urls')
df_base_url=df[df['url base']=='https://ar.blackanddecker.global/productos/herramientas-electricas?page=num_pag']

def scrapear_base_url(df_base_url):
    df_final = pd.DataFrame()
    for idx, fila in df_base_url.iterrows():
        df_result = scrapear_productos(
        url_base=fila['url base'],
        xpath_link_sku=fila['Ruta Xpath link'],
        xpath_sku=fila['Ruta Xpath sku'],
        xpath_brand=fila['Ruta Xpath brand'],
        xpath_name_product=fila['Ruta Xpath nombre producto'],
        xpath_description_product=fila['Ruta Xpath descripcion'],
        xpath_image_product=fila['Ruta Xpath imagen'],
        xpath_category_product=fila['Ruta Xpath categoria'],
        xpath_esp_tecnic_product=fila['Ruta Xpath esp tecnica'],
        xpath_price_product=fila['Ruta Xpath precio'],
        xpath_page=fila['Ruta Xpath paginacion'], 
        country=fila['Country'],
        name_page=fila['Name'],
        information=fila['Information'],
        secuencia=fila['Secuencia de paginacion']
        )
        print(df_result.head())
        df_final = pd.concat([df_final, df_result], ignore_index=True)
        print(f"Scrapeo completado para la url base: {fila['url base']}")
        print(df_final.head())
    return df_final  


if __name__ == "__main__":
    print("Columnas del DataFrame:", df.columns)  # Esto te ayuda a depurar nombres de columnas
    df_resultado = scrapear_base_url(df_base_url)
    print(df_resultado)
    # Si quieres guardar el resultado en un Excel:
    df_resultado.to_excel("productos_blackanddecker.xlsx", index=False)
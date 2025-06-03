

"""
extraction.py

Este módulo contiene funciones para la extracción de enlaces de productos desde páginas web paginadas.
Incluye la función principal extraer_links_url, que recorre todas las páginas de una URL base, detecta los enlaces de productos y los retorna en una lista.

Funciones:
- extraer_links_url: Extrae enlaces de productos de todas las páginas asociadas a una URL base paginada.

Dependencias externas:
- requiere funciones y patrones definidos en los módulos config.py, network.py y link_web_paginda.py.
"""


from packages import List,time
from .config import posibles_subcadenas
from .network import  validar_url
from .link_web_paginda import obtener_links_web_paginada
from .link_web_scroll import scrapear_productos, HEADERS
def extraer_links_url(url_base: str, sec_pag: int, posibles_subcadenas: List[str], pausa: int = 10, max_errores: int = 2) -> List[str]:
    """
    Extrae enlaces de productos de todas las páginas asociadas a una URL base paginada.

    Parámetros:
    - url_base: string con 'num_pag' como marcador para el número de página.
    - sec_pag: incremento por página (1, 10, etc.).
    - posibles_subcadenas: lista de palabras clave que indican que un link es de producto.
    - pausa: segundos de espera entre solicitudes (default: 10).
    - max_errores: número máximo de fallos consecutivos antes de detener (default: 2).

    Retorna:
    - Lista de URLs extraídas.
    """
    
    errores_consecutivos = 0
    numero_pagina = 0
    links_extraidos = []

    while errores_consecutivos < max_errores:
        # Reemplazo del marcador en la URL
        url = url_base.replace('num_pag', str(numero_pagina)) if 'num_pag' in url_base else url_base

        estado, respuesta = validar_url(url)
        print(f'{"-"*100}\n')
        print(f'estado de la url {url} es :{estado}\n')
        if estado <=1:  # url con o sin paginacion
            links_pagina = obtener_links_web_paginada(respuesta, url, posibles_subcadenas)
            print(f"[✅] Página {numero_pagina}: {len(links_pagina)} links encontrados en {url}\n")

            if not links_pagina:
                print(f"[🛑] Página {numero_pagina} sin contenido. Deteniendo scraping.\n")
                break

            if estado == 1:
                errores_consecutivos +=1
                print(f'la url :{url} en su pagina {sec_pag} no tiene informacion, numero de errores {errores_consecutivos}\n')
            else:
                errores_consecutivos +=0   # Resetear contador de errores
                numero_pagina += sec_pag
    
            url_new=url_base.replace('num_pag', str(numero_pagina)) if 'num_pag' in url_base else url_base
            _, respuesta_new = validar_url(url)
           
            if url==url_new or respuesta==respuesta_new:
                errores_consecutivos+=1
                print(f'la url :{url} ya se evaluo, numero de errores {errores_consecutivos}\n')
            else:
                print(f'la respuesta de :{url}  cambio\n')
                print(f'url inicial _{url}\n')
                print(f'url new _{url_new}\n')
                print(f'las urls son iguales ? {url==url_new} ')
            links_extraidos.extend(links_pagina)
    
    
        elif estado == 2  and 'ferrepat.com' in url:   # URL con scroll
            
            links_pagina = scrapear_productos(url_base,HEADERS)
            links_extraidos.extend(links_pagina)
            errores_consecutivos+=2
            print(f'la url : {url} ya se evaluo en su totalidad\n')       
        
        else:
            errores_consecutivos += 1
            print(f' estado para {url} es :{estado}\n')
            print(f"[⚠️] Error en {url}. Intento {errores_consecutivos} de {max_errores}\n")
        print(f'{"-"*100}\n')
        time.sleep(pausa)

    return list(set(links_extraidos))


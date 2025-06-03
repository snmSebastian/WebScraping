from packages import BeautifulSoup
from urllib.parse import urljoin
from .config import posibles_subcadenas       
from .config import PATRONES_FILTROS, PATRONES_PRODUCTO, ATRIBUTOS_PRODUCTO



#==========================
#--- Es_link_de_producto
#========================== 
def es_link_de_producto(a_tag, posibles_subcadenas):
    '''Verifica si un enlace es un link de producto
    arg:
        * a_tag: etiqueta <a> de BeautifulSoup
        * posibles_subcadenas: patrones comunes en URLs de productos
    return: True si es un link de producto, False en caso contrario
    '''
    href = a_tag['href'].lower()
    clases = a_tag.get('class', [])
    atributos = a_tag.attrs

    # Condición: contiene palabra clave
    contiene_subcadena = any(sub in href for sub in posibles_subcadenas)

    # Condición: clase relacionada a producto
    clase_relevante = any(PATRONES_PRODUCTO.search(clase) for clase in clases)

    # Condición: atributos HTML de producto
    tiene_atributo = any(attr in atributos for attr in ATRIBUTOS_PRODUCTO)

    # Condición: no es un filtro ni ancla
    no_es_filtro = not PATRONES_FILTROS.search(href)

    return (contiene_subcadena or clase_relevante or tiene_atributo) and no_es_filtro


#==========================
#--- Obtener_links_web_paginada
#==========================
def obtener_links_web_paginada(respuesta, url, posibles_subcadenas):
    '''Obtiene los links de productos de una pagina paginada
    arg:
        * respuesta: respuesta de la peticion
        * url: url base
        * posibles_subcadenas: patrones comunes en URLs de productos
    return: lista de links
    '''
    soup = BeautifulSoup(respuesta.text, 'html.parser')
    links = set()

    for a in soup.find_all('a', href=True):
        if es_link_de_producto(a, posibles_subcadenas):
            full_url = urljoin(url, a['href'])
            links.add(full_url)

    return list(links)


"""
network.py

Este módulo contiene funciones relacionadas con la interacción de red para el proceso de webscraping.
Incluye utilidades para validar URLs y detectar la presencia de paginación en páginas web.

Funciones:
- contiene_paginacion(soup): Verifica si el HTML contiene elementos que indiquen paginación.
- validar_url(url, session, timeout): Valida el acceso a una URL y determina si tiene paginación.

Dependencias externas:
- session y user_agents deben ser importados desde el módulo config.py.
- BeautifulSoup y random deben estar disponibles en el entorno.
"""


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from .config import session, user_agents
from packages import BeautifulSoup, random
#==========================
#--- Contiene_paginacion
#==========================
def contiene_paginacion(soup):
    '''Verifica si alguna etiqueta <a> o button contiene indicios de paginacion
    arg :soup
    return valor(0,1,2) respuesta
    '''
    # Lista extendida y homogénea (en minúsculas)
    palabras_paginacion = [
        'siguiente', 'sig', '›', '»', '→', 'adelante', 'próximo','paginacion',
        'next', 'forward', '>', '>>', 'seguinte','pagination','page','pag'
    ]

    # Buscar todos los enlaces y botones con texto que podría ser paginación
    elementos = soup.find_all(['a', 'button', 'li'])

    for el in elementos:
        texto = el.get_text(strip=True).lower()
        if any(p in texto for p in palabras_paginacion):
            print(f'contiene paginacion')
            return True
        
    return False


#==========================
#--- Verifica_scroll        
#=========================
def verifica_scroll(url, tiempo_espera=2, repeticiones=2):
    """
    Verifica si una página tiene scroll vertical y si presenta scroll infinito.

    Parámetros:
        url (str): URL de la página a analizar.
        tiempo_espera (int): Tiempo de espera entre scrolls en segundos.
        repeticiones (int): Número de intentos de scroll para verificar cambios.

    Retorna:
        dict: {'tiene_scroll': bool, 'scroll_infinito': bool}
    """
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    resultado = {'tiene_scroll': False, 'scroll_infinito': False}

    try:
        driver.get(url)
        time.sleep(tiempo_espera)  # Esperar carga inicial

        scroll_height = driver.execute_script("return document.body.scrollHeight")
        client_height = driver.execute_script("return window.innerHeight")
        resultado['tiene_scroll'] = scroll_height > client_height

        last_height = scroll_height

        for _ in range(repeticiones):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(tiempo_espera)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height > last_height:
                resultado['scroll_infinito'] = True
                break
            last_height = new_height

    finally:
        driver.quit()

    return resultado['scroll_infinito'] or resultado['tiene_scroll']# True si tiene scroll infinito, False si no lo tiene




#==========================
#--- Validar_url
#==========================
def validar_url(url, session=session, timeout=20):
    '''Valida cada url paginada para saber si se puede o no ingresar a esta, y ademas si esta es o no la ultima pagina, esto mediante la verificacion si tiene o no un boton que permita cambiar
     de pag
      arg: 
        * url: url paginada
         *session:mantiene la sesion activa
          *timeout: tiempo de espera
     '''
    try:
        headers = {
            'User-Agent': random.choice(user_agents),
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
        }
        respuesta = session.get(url, headers=headers, verify=False, timeout=timeout, allow_redirects=True)

        if respuesta.status_code == 200:
            soup = BeautifulSoup(respuesta.text, 'html.parser')
            
            if contiene_paginacion(soup):
                return 0, respuesta #puede ingresar a la url y tiene paginacion
            elif verifica_scroll(url):
                return 2, respuesta#puede ingresar a la url y tiene scroll infinito
            else:       
                return 1, respuesta #puede ingresar a la url y no tiene paginacion
        else:   
            print(f"[❌] Error al acceder a {url}: {respuesta.status_code}")
            return 3, None
  
    except Exception as e:
        return 4, None



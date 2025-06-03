import requests
from bs4 import BeautifulSoup
import csv
import time
# FUNCIONA 

url_base = "https://www.ferrepat.com/catalogo-de-productos-dewalt"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:138.0) Gecko/20100101 Firefox/138.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.ferrepat.com",
    "Referer": url_base,
}


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time

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
#--- Detecta_scroll_infinito
#==========================

def obtener_token(session, base_url, headers):
    """
    Obtiene el token CSRF de la página inicial.
    """
    resp = session.get(base_url, headers=headers)
    soup = BeautifulSoup(resp.text, "html.parser")
    token_tag = soup.find("input", {"name": "_token"})
    if token_tag:
        token = token_tag["value"]
        print(f"✅ Token extraído: {token}")
        return token
    else:
        raise Exception("❌ No se encontró el token CSRF")


#==========================
#--- Extraer_links
#==========================
def extraer_productos(html_list):
    productos = []
    for item in html_list:
        soup = BeautifulSoup(item, "html.parser")
        a_tag = soup.find("a", href=True)
        if a_tag:
            href = a_tag["href"]
            # Si el href ya es absoluto, úsalo tal cual; si es relativo, antepone el dominio
            if href.startswith("http"):
                link = href
            else:
                link = "https://www.ferrepat.com" + href
            productos.append(link)
    return productos



#==========================
#--- Extraer_links
#==========================
def scrapear_productos(base_url, headers):
    """
    Extrae los links de productos de todas las páginas usando POST y paginación.
    """
    session = requests.Session()
    token = obtener_token(session, base_url, headers)

    payload = {
        "_token": token,
        "pagina": 1
    }

    print("📄 Obteniendo página inicial...")
    r = session.post(base_url, data=payload, headers=headers)
    data = r.json()

    productos = extraer_productos(data["products"])
    total_pages = int(data["total_pages"])
    print(f"🔁 Total de páginas: {total_pages}")

    for page in range(2, total_pages + 1):
        print(f"📄 Página {page} de {total_pages}")
        payload["pagina"] = page
        time.sleep(1.5)
        r = session.post(base_url, data=payload, headers=headers)
        data = r.json()
        productos += extraer_productos(data["products"])

    print(f"✅ Total productos extraídos: {len(productos)}")
    return productos

#x=scrapear_productos(url_base,HEADERS)
#print(x[:5])

''' 
Este archivo contiene la configuración global del proyecto de webscraping.
 Incluye:
 - posibles_subcadenas: patrones comunes en URLs de productos para identificar enlaces relevantes.
 - session: objeto Session de requests para optimizar las solicitudes HTTP.
 - user_agents: lista de user-agents para rotar en las solicitudes y evitar bloqueos.
 Centralizar esta configuración facilita el mantenimiento y la reutilización en todo el proyecto.
'''

from packages import requests,re


# Verificacion de si es producto
posibles_subcadenas = [
    # Generales en e-commerce
    '/product/', '/products/', '/producto/', '/productos/',
    '/item/', '/items/', '/detalle/', '/detail/',
    '/sku/', '/articulo/', '/artículos/',

    # Términos comunes en español e inglés
    '/detalle-producto/', '/ver-producto/', '/ver_producto/',
    '/viewproduct/', '/product-detail/', '/product_info/',

    # Prefijos o patrones típicos
    '-p-', '-prod-', '-item-', '-sku-', '-detalle-',

    # Patrones Amazon
    '/dp/', '/gp/product/',

    # Patrones MercadoLibre (por país)
    '/mla-', '/mlm-', '/mlc-', '/mlv-', '/mlu-', '/mlb-', '/mls-',  # Argentina, México, Chile, Venezuela, Uruguay, Brasil, Colombia

    # Easy y Sodimac (comunes en LATAM)
    '/producto/', '/productos/', '/sku/', '/ficha/', '/ficha-producto/',

    # Stanley, DeWalt, Bosch, Makita, etc.
    '/tools/', '/catalog/', '/categories/', '/details/', '/item-details/',

    # Otros posibles patrones semánticos
    '/shop/', '/buy/', '/comprar/', '/oferta/', '/ofertas/', '/promo/', '/promocion/',
    
    # Extensiones finales sospechosas
    '.html', '.htm'
  ]

# validar_url
session = requests.Session()
user_agents = [
    # Lista simple para rotar headers
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)...',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64)...',
    # Puedes expandirla con más UA reales
]



# Precompilamos patrones
PATRONES_PRODUCTO = re.compile(r'(product|item|skcard|detail)', re.I)
PATRONES_FILTROS = re.compile(r'(category|categories|filter|#)', re.I)
ATRIBUTOS_PRODUCTO = ['data-product-id', 'data-sku', 'data-id', 'data-item-id']
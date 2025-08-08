# -*- coding: utf-8 -*-
"""
Created on Fri Jun 27 12:23:02 2025

@author: walter.schneider
"""


import time
import random
import os
import requests
import pandas as pd
from datetime import datetime
import json
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt

# Se indica Directorio
directorio = r"C:\Users\walter.schneider\Documents\CSV\Komatsu"
os.chdir(directorio)

# ====== CONFIGURACIÓN ======
USERNAME = "296969_$)m;/?UY"
PASSWORD = "%fIs(1U&3hyzDLh{"
SUBSCRIBER_ID = "296969"
PAGE = 1
BASE_URL = "https://isoapi.komtrax.komatsu"

""" FUNCIONES """

def obtener_datos_snapshot(token, subscriber_id, page=1):
    """
    Consulta los datos más recientes (snapshot) de la flota.
    """
    url = f"{BASE_URL}/provider/v1/{subscriber_id}/Fleet/{page}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error al obtener datos: {response.status_code}\n{response.text}")

def obtener_datos_timeseries(token, subscriber_id, make_code, model,serial, dataitem, startdate, enddate, page=1):
    """
    Consulta datos históricos (Time Series) de un ítem específico para una máquina.

    Parámetros:
    - token: str → Token de autenticación
    - subscriber_id: str → ID del suscriptor
    - make_code: str → Código del fabricante (para Komatsu es "0001")
    - modelo: str → Modelo del equipo (ej. "PC200-10")
    - serie: str → Número de serie del equipo (ej. "A12345")
    - data_item: str → Nombre del ítem a consultar (ej. "CumulativeOperatingHours", "FuelUsedInThePreceding24Hours")
    - fecha_inicio: str → Fecha de inicio (formato "YYYY-MM-DD")
    - fecha_termino: str → Fecha de término (formato "YYYY-MM-DD")
    - page: int → Número de página (por defecto 1)

    Retorna:
    - dict con los datos de respuesta
    """
    base_url = "https://isoapi.komtrax.komatsu"
    endpoint = f"{base_url}/provider/v1/{subscriber_id}/Fleet/Equipment/MakeModelSerial/{make_code}/{model}/{serial}/{dataitem}/{startdate}/{enddate}/{page}"
    
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error al obtener datos Time Series: {response.status_code}\n{response.text}")


""" TOKEN """

# URL de autenticación
auth_url = "https://isoapi.komtrax.komatsu/provider/token"

# Credenciales (reemplaza por las reales)

# Construcción del cuerpo de la petición (url encoded)
payload = {"grant_type": "password","Username": USERNAME,"password": PASSWORD}

# Encabezados
headers = {"Content-Type": "application/x-www-form-urlencoded"}

# Solicitar el token
response = requests.post(auth_url, data=payload, headers=headers)

if response.status_code == 200:
    token = response.json().get("access_token")
    print("Token obtenido:", token)
else:
    raise Exception(f"Error al obtener token: {response.status_code}\n{response.text}")


""" EJECUCIÓN DATOS """

try:
    print("📡 Solicitando datos...")
    datos = obtener_datos_snapshot(token, SUBSCRIBER_ID, page=PAGE)
    print("✅ Datos recibidos.")
except Exception as e:
    print("❌ Error:", str(e))

""" CREAR DATAFRAME CON DATOS DE MAQUINAS """
fleet_list = datos.get("Equipment", [])

lista = []

for maq in fleet_list:
    sigla = maq.get("EquipmentHeader", {}).get("EquipmentID")
    modelo = maq.get("EquipmentHeader", {}).get("Model")
    serie = maq.get("EquipmentHeader", {}).get("SerialNumber")
    lista.append({'Sigla': sigla, 'Model': modelo, 'Serial': serie})
    
df_maq = pd.DataFrame(lista)

""" CONSULTA POR SIGLA """
region = 16
sigla = '16N-MMKO-882'
data_item = "Locations"
fecha_inicio = "2024-01-01"
fecha_termino = "2025-12-01"

make_code = "0001"
modelo = df_maq[df_maq['Sigla'] == sigla]['Model'].values[0]
serie = df_maq[df_maq['Sigla'] == sigla]['Serial'].values[0]

#DATOS SNAPSHOT
maq = next((item for item in fleet_list if item.get("EquipmentHeader", {}).get("EquipmentID") == sigla), None)
ubicacion = maq.get("Location", {})
lat = ubicacion.get("Latitude")
lon = ubicacion.get("Longitude")
df_loc_today = pd.DataFrame.from_dict({'0': {"datetime": datetime, "Latitude": lat, "Longitude": lon}}, orient='index')

#DATOS TIMESERIES
datos_ts = obtener_datos_timeseries(token, SUBSCRIBER_ID, make_code, modelo, serie, data_item, fecha_inicio, fecha_termino, page=PAGE)
ubicaciones = datos_ts.get('Location')

loc = []

for ubicacion in ubicaciones:
    datetime = ubicacion.get('datetime')
    lat = ubicacion.get('Latitude')
    lon = ubicacion.get('Longitude')
    loc.append({'datetime': datetime, 'Latitude': lat, 'Longitude': lon})
    
df_loc = pd.DataFrame(loc)

#POST
url = "http://127.0.0.1:5000/ingest"
machine_id = sigla

df_loc.to_excel('locaciones.xlsx')

mem_bytes = len(json.dumps(ubicaciones).encode('utf-8'))
print(f"{mem_bytes/1024:.2f} KB totales")

for index, row in df_loc.iterrows():
    payload = {
        "machine_id": machine_id,
        "lat": row['Latitude'],
        "lon": row['Longitude'],
        "ts": row['datetime']
    }   
    try:
        r = requests.post(url, json=payload)
        print(f"{index+1}/5 →", r.status_code, r.json())
    except Exception as e:
        print("Error:", e)
    time.sleep(5)  # envía cada 5 s para acelerar la demo

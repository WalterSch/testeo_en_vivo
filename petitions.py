# -*- coding: utf-8 -*-
"""
Created on Fri Jun 27 12:38:28 2025

@author: walter.schneider
"""
import requests
import json

url = 'http://127.0.0.1:5000/machines/16N-MMKO-882/data'
response = requests.get(url)
data = response.json()

mem_bytes = len(json.dumps(data).encode('utf-8'))
print(f"{mem_bytes/1024:.2f} KB totales")

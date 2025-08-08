# -*- coding: utf-8 -*-
"""
Created on Fri Jun 27 12:17:28 2025

@author: walter.schneider
"""

from flask import Flask, request, jsonify

app = Flask(__name__)

DATA = []  # Almacenamiento temporal en memoria

@app.route('/')
def root():
    return "Home"

@app.route('/ingest', methods=['POST'])
def ingest():
    payload = request.get_json()
    required = ['machine_id', 'lat', 'lon', 'ts']
    if not all(k in payload for k in required):
        return jsonify({'error': 'missing field'}), 400
    DATA.append(payload)
    return jsonify({'status': 'ok'}), 201

@app.route('/machines/<mid>/data', methods=['GET'])
def get_data(mid):
    return jsonify([d for d in DATA if d['machine_id'] == mid])

if __name__ == '__main__':
    app.run(port=5000, debug=True)

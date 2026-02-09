import requests
#import json
from datetime import datetime as dt, timedelta, UTC
import pandas as pd

# Lista de sensores
sensors = [
    {"idSensor": 1, "description": "Battery"},
    {"idSensor": 2, "description": "Carbon Monoxide"},
    {"idSensor": 3, "description": "Relative Humidity"},
    {"idSensor": 4, "description": "Location"},
    {"idSensor": 7, "description": "Ozone"},
    {"idSensor": 8, "description": "PM10*"},
    {"idSensor": 9, "description": "PM2.5"},
    {"idSensor": 12, "description": "Temperature"},
    {"idSensor": 13, "description": "Device Mode"},
    {"idSensor": 1000, "description": "IAS"},
    {"idSensor": 1001, "description": "AQI"}
]

#para evitar rechazos de peticiones que no son de un navegador
headers = {
    "User-Agent": "Mozilla/5.0"
}


def get_air_quality_data(sensor_id, token, timeDeltaArgKey, timeDeltaArgValue, timezone_offset_hours=-6):
    current_time = dt.now(UTC) + timedelta(hours=timezone_offset_hours)
    dtEnd = current_time.strftime('%Y-%m-%d %H:%M:%S')
    dtStart = (current_time - timedelta(**{timeDeltaArgKey: timeDeltaArgValue})).strftime('%Y-%m-%d %H:%M:%S')

    dtEnd_encoded = dtEnd.replace(' ', '%20')
    dtStart_encoded = dtStart.replace(' ', '%20')

    api_url = (f'https://smability.sidtecmx.com/SmabilityAPI/GetData?token={token}'
               f'&idSensor={sensor_id}&dtStart={dtStart_encoded}&dtEnd={dtEnd_encoded}')

    #print("URL:", api_url)

    try:
        response = requests.get(url=api_url, headers=headers, timeout=20)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Error al conectar:", e)
        return []


def get_hourly_air_quality(sensor_id, token, hours, timezone_offset_hours=-6):
    current_time = dt.now(UTC) + timedelta(hours=timezone_offset_hours)
    end_time = current_time.replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=hours)

    dtEnd_encoded = end_time.strftime('%Y-%m-%d %H:%M:%S').replace(' ', '%20')
    dtStart_encoded = start_time.strftime('%Y-%m-%d %H:%M:%S').replace(' ', '%20')

    api_url = (f'https://smability.sidtecmx.com/SmabilityAPI/GetData?token={token}'
               f'&idSensor={sensor_id}&dtStart={dtStart_encoded}&dtEnd={dtEnd_encoded}')

   # print("URL:", api_url)

    try:
        response = requests.get(url=api_url, headers=headers, timeout=20)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print("Error al conectar:", e)
        return {}

    if not data or not isinstance(data, list):
        return {"error": "No hay datos disponibles o formato invalido"}

    hourly_data = {}
    for entry in data:
        try:
            timestamp = dt.strptime(entry['TimeStamp'], '%Y-%m-%dT%H:%M:%S')
            value = float(entry['Data'])
        except (ValueError, KeyError) as e:
            continue

        hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
        hourly_data.setdefault(hour_start, []).append(value)

    hourly_averages = {}
    for hour, values in hourly_data.items():
        if values:
            hourly_averages[hour.strftime('%Y-%m-%d %H:%M:%S')] = round(sum(values) / len(values), 2)

    result = {}
    for i in range(hours):
        target_hour = end_time - timedelta(hours=i + 1)
        key = target_hour.strftime('%Y-%m-%d %H:%M:%S')
        result[key] = hourly_averages.get(key, None)

    return result


# Inicialización de parámetros
token = '005cdaeb7b1392ad59a4335f4a832043'
hours = 6024
timeDeltaArgKey = "minutes"
timeDeltaArgValue = 72288  # 48 horas en minutos


def main():
    for sensor in sensors:
        sensor_id = sensor["idSensor"]
       # print(f'\nConsultando datos de {sensor["description"]} (ID: {sensor_id})')

        raw_data = get_air_quality_data(sensor_id, token, timeDeltaArgKey, timeDeltaArgValue)
        print(f"{sensor['description']} muestras: {len(raw_data)}")

        if raw_data:
            df = pd.DataFrame(raw_data)
            df.to_csv(f'air_quality_data_{sensor_id}.csv', index=False)
            print(f"Datos guardados: air_quality_data_{sensor_id}.csv")

        hourly_data = get_hourly_air_quality(sensor_id, token, hours)
        if hourly_data:
            df = pd.DataFrame(list(hourly_data.items()), columns=["Timestamp", "Data"])
            df.to_csv(f'get_hourly_air_quality_{sensor_id}.csv', index=False)
            print(f"Datos horarios guardados: get_hourly_air_quality_{sensor_id}.csv")


if __name__ == "__main__":
    main()

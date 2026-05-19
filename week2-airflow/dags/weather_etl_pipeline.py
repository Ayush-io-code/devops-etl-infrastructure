"""
Weather ETL Pipeline
====================
Industry-style DAG that:
  1. EXTRACT  — fetches live weather data from Open-Meteo API (free, no API key needed)
  2. TRANSFORM — cleans, calculates feels-like category, flags extreme temps
  3. LOAD      — saves processed data to a CSV report

Real-world concepts demonstrated:
  - XCom for passing data between tasks
  - Error handling in each task
  - Parameterised cities (easy to extend)
  - Data validation before load
  - Audit columns (processed_at, pipeline_run_id)
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import urllib.request
import csv
import os

# ── Config ────────────────────────────────────────────────────────────────────
CITIES = {
    "Mumbai":    {"lat": 19.0760, "lon": 72.8777},
    "Delhi":     {"lat": 28.6139, "lon": 77.2090},
    "Bangalore": {"lat": 12.9716, "lon": 77.5946},
    "Chennai":   {"lat": 13.0827, "lon": 80.2707},
    "Kolkata":   {"lat": 22.5726, "lon": 88.3639},
}

OUTPUT_DIR = "/opt/airflow/logs/weather_reports"
# ─────────────────────────────────────────────────────────────────────────────


def extract(**context):
    """
    Fetch current weather for each city from Open-Meteo (free, no API key).
    Pushes raw JSON list to XCom.
    """
    raw_records = []

    for city, coords in CITIES.items():
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={coords['lat']}&longitude={coords['lon']}"
            f"&current=temperature_2m,relative_humidity_2m,"
            f"wind_speed_10m,precipitation,weather_code"
            f"&timezone=Asia%2FKolkata"
        )
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                current = data["current"]
                raw_records.append({
                    "city":        city,
                    "latitude":    coords["lat"],
                    "longitude":   coords["lon"],
                    "temperature": current["temperature_2m"],
                    "humidity":    current["relative_humidity_2m"],
                    "wind_speed":  current["wind_speed_10m"],
                    "precipitation": current["precipitation"],
                    "weather_code": current["weather_code"],
                    "observed_at": current["time"],
                })
                print(f"[EXTRACT] {city}: {current['temperature_2m']}°C")
        except Exception as e:
            print(f"[EXTRACT] WARNING: Failed to fetch {city}: {e}")

    if not raw_records:
        raise ValueError("No data extracted — all API calls failed")

    print(f"[EXTRACT] Done. {len(raw_records)} cities fetched.")
    context["ti"].xcom_push(key="raw_weather", value=raw_records)


def transform(**context):
    """
    Clean and enrich the raw records:
      - Map weather codes to human-readable conditions
      - Classify temperature into comfort bands
      - Flag extreme weather
      - Add audit columns
    """
    raw_records = context["ti"].xcom_pull(key="raw_weather", task_ids="extract")

    if not raw_records:
        raise ValueError("No raw data received from extract task")

    # WMO Weather Code mapping (subset of common codes)
    WMO_CODES = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Foggy", 48: "Icy fog",
        51: "Light drizzle", 53: "Drizzle", 55: "Heavy drizzle",
        61: "Light rain", 63: "Rain", 65: "Heavy rain",
        71: "Light snow", 73: "Snow", 75: "Heavy snow",
        80: "Light showers", 81: "Showers", 82: "Heavy showers",
        95: "Thunderstorm", 96: "Thunderstorm with hail",
    }

    def temp_category(t):
        if t < 10:   return "Cold"
        if t < 20:   return "Cool"
        if t < 28:   return "Comfortable"
        if t < 35:   return "Warm"
        return "Hot"

    def is_extreme(record):
        return (
            record["temperature"] > 40 or
            record["temperature"] < 5  or
            record["wind_speed"] > 60  or
            record["precipitation"] > 20
        )

    run_id = context["run_id"]
    processed_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    transformed = []

    for r in raw_records:
        transformed.append({
            "city":             r["city"],
            "latitude":         r["latitude"],
            "longitude":        r["longitude"],
            "temperature_c":    r["temperature"],
            "humidity_pct":     r["humidity"],
            "wind_speed_kmh":   r["wind_speed"],
            "precipitation_mm": r["precipitation"],
            "condition":        WMO_CODES.get(r["weather_code"], "Unknown"),
            "temp_category":    temp_category(r["temperature"]),
            "extreme_weather":  is_extreme(r),
            "observed_at":      r["observed_at"],
            "processed_at":     processed_at,
            "pipeline_run_id":  run_id,
        })
        print(
            f"[TRANSFORM] {r['city']}: {r['temperature']}°C "
            f"→ {temp_category(r['temperature'])} | "
            f"{WMO_CODES.get(r['weather_code'], 'Unknown')}"
        )

    extreme_cities = [r["city"] for r in transformed if r["extreme_weather"]]
    if extreme_cities:
        print(f"[TRANSFORM] ⚠ Extreme weather flagged in: {', '.join(extreme_cities)}")

    context["ti"].xcom_push(key="transformed_weather", value=transformed)
    print(f"[TRANSFORM] Done. {len(transformed)} records transformed.")


def validate(**context):
    """
    Basic data quality checks before loading.
    Fails the pipeline if critical checks don't pass.
    """
    records = context["ti"].xcom_pull(key="transformed_weather", task_ids="transform")

    if not records:
        raise ValueError("No transformed data to validate")

    errors = []
    for r in records:
        if r["temperature_c"] < -90 or r["temperature_c"] > 60:
            errors.append(f"{r['city']}: temperature out of range ({r['temperature_c']})")
        if not (0 <= r["humidity_pct"] <= 100):
            errors.append(f"{r['city']}: humidity out of range ({r['humidity_pct']})")

    if errors:
        raise ValueError(f"Validation failed:\n" + "\n".join(errors))

    print(f"[VALIDATE] All {len(records)} records passed validation.")


def load(**context):
    """
    Save transformed data to a dated CSV file.
    In production this would be: database INSERT, S3 upload, API call, etc.
    """
    records = context["ti"].xcom_pull(key="transformed_weather", task_ids="transform")

    if not records:
        raise ValueError("No data to load")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    output_path = os.path.join(OUTPUT_DIR, f"weather_report_{date_str}.csv")

    fieldnames = [
        "city", "latitude", "longitude",
        "temperature_c", "humidity_pct", "wind_speed_kmh",
        "precipitation_mm", "condition", "temp_category",
        "extreme_weather", "observed_at", "processed_at", "pipeline_run_id",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"[LOAD] Report saved to: {output_path}")
    print(f"[LOAD] {len(records)} records written.")

    # Print summary table to logs
    print("\n── Weather Summary ──────────────────────────────")
    print(f"{'City':<12} {'Temp':>6} {'Humidity':>9} {'Condition':<20} {'Category'}")
    print("-" * 65)
    for r in records:
        flag = " ⚠" if r["extreme_weather"] else ""
        print(
            f"{r['city']:<12} {r['temperature_c']:>5}°C "
            f"{r['humidity_pct']:>8}% "
            f"{r['condition']:<20} {r['temp_category']}{flag}"
        )
    print("─────────────────────────────────────────────────\n")


# ── DAG Definition ────────────────────────────────────────────────────────────
default_args = {
    "owner": "ayush",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="Extract live weather → transform → validate → load to CSV",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl", "weather", "demo"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t_extract >> t_transform >> t_validate >> t_load

import time
import psutil
import clickhouse_connect
import psycopg2
from openpyxl import Workbook, load_workbook

# Konfiguracja połączenia z ClickHouse
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = '1234'
CLICKHOUSE_DATABASE = 'default'

# Konfiguracja połączenia z PostgreSQL
PG_HOST = 'localhost'
PG_PORT = 5432
PG_USER = 'postgres'
PG_PASSWORD = '1234'
PG_DATABASE = 'AirFlights'

# Funkcja monitorująca zużycie CPU i RAM przed, w trakcie i po zapytaniu
def monitor_resources():
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    return cpu_usage, memory_usage

# Funkcja wykonująca zapytanie w ClickHouse i mierząca czas oraz zasoby
def execute_clickhouse_query(query):
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD
    )

    # Pomiar czasu połączenia
    connect_start = time.time()
    client.ping()
    connect_time = time.time() - connect_start

    # Pomiar czasu zapytania
    cpu_before, memory_before = monitor_resources()
    query_start = time.time()
    result = client.query(query).result_rows
    query_time = time.time() - query_start
    cpu_after, memory_after = monitor_resources()

    client.close()

    return {
        "result": result,
        "execution_time": query_time,  # Czas samego zapytania
        "connect_time": connect_time,  # Czas na połączenie
        "cpu_before": cpu_before,
        "cpu_after": cpu_after,
        "memory_before": memory_before,
        "memory_after": memory_after
    }


# Funkcja wykonująca zapytanie w PostgreSQL i mierząca czas oraz zasoby
def execute_pg_query(query):
    # Opóźnienie o 5 sekund przed wykonaniem zapytania
    time.sleep(10)

    # Mierzymy czas połączenia
    connect_start = time.time()
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        database=PG_DATABASE
    )
    connect_time = time.time() - connect_start

    cursor = conn.cursor()

    # Mierzymy czas wysyłania zapytania
    cpu_before, memory_before = monitor_resources()
    query_start = time.time()

    cursor.execute(query)
    result = cursor.fetchall()
    query_time = time.time() - query_start

    cpu_after, memory_after = monitor_resources()
    cursor.close()
    conn.close()

    return {
        "result": result,
        "execution_time": query_time,  # Czas samego zapytania
        "connect_time": connect_time,  # Czas na połączenie
        "cpu_before": cpu_before,
        "cpu_after": cpu_after,
        "memory_before": memory_before,
        "memory_after": memory_after
    }


# Funkcja zapisująca wyniki do pliku Excel
# Funkcja zapisująca wyniki do pliku Excel
def save_results_to_excel(ch_results, pg_results):
    try:
        wb = load_workbook("query_comparison.xlsx")
        ws = wb.active
    except FileNotFoundError:
        wb = Workbook()
        ws = wb.active
        ws.title = "Query Comparison"
        ws.append([
            "Database", "Execution Time (s)", "Connection Time (s)",
            "CPU Before (%)", "CPU After (%)",
            "Memory Before (%)", "Memory After (%)", "Result Rows"
        ])

    # Wyniki dla ClickHouse
    ws.append([
        "ClickHouse", ch_results["execution_time"], ch_results["connect_time"],
        ch_results["cpu_before"], ch_results["cpu_after"],
        ch_results["memory_before"], ch_results["memory_after"],
        len(ch_results["result"])
    ])

    # Wyniki dla PostgreSQL
    ws.append([
        "PostgreSQL", pg_results["execution_time"], pg_results["connect_time"],
        pg_results["cpu_before"], pg_results["cpu_after"],
        pg_results["memory_before"], pg_results["memory_after"],
        len(pg_results["result"])
    ])

    wb.save("query_comparison.xlsx")
    print("Wyniki zapisane w pliku query_comparison.xlsx")


# Przykładowe zapytanie
query = "SELECT f.FLIGHT_NUMBER, f.SCHEDULED_DEPARTURE, f.DEPARTURE_DELAY, f.ARRIVAL_DELAY, o.IATA_CODE AS ORIGIN_AIRPORT, o.CITY AS ORIGIN_CITY, o.COUNTRY AS ORIGIN_COUNTRY, d.IATA_CODE AS DESTINATION_AIRPORT, d.CITY AS DESTINATION_CITY, d.COUNTRY AS DESTINATION_COUNTRY FROM flights f JOIN Airports o ON f.ORIGIN_AIRPORT = o.IATA_CODE JOIN Airports d ON f.DESTINATION_AIRPORT = d.IATA_CODE WHERE f.DEPARTURE_DELAY > 30;"


# Wykonanie zapytania w ClickHouse i PostgreSQL oraz zapisanie wyników do Excel
clickhouse_results = execute_clickhouse_query(query)
pg_results = execute_pg_query(query)
save_results_to_excel(clickhouse_results, pg_results)


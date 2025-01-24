from Modules.path_functions import data_paths
from Modules.spark_functions import get_data, get_SparkSession, process_data
from Modules.database_postgres import execute_sql_files, get_connection, get_results
from dotenv import load_dotenv
import logging
import os

def get_environment_variables():
    """
    Loads environment variables from a `.env` file and returns a dictionary 
    with the database configuration parameters.

    Returns:
        dict: A dictionary containing the following database parameters:
            - "DB_HOST" (str): Database server host.
            - "DB_USER" (str): Database user.
            - "DB_PASS" (str): Database user password.
            - "DB_NAME" (str): Database name.
            - "DB_SCHEMA" (str): Database schema.
            - "DB_PORT" (str): Database server port.
            - "DB_TABLE" (str): Database table name.

    Notes:
        - The `.env` file must be properly configured with the keys matching 
          the variables used in this function.
    """
    load_dotenv()
    database_parameters = {
            "DB_HOST": os.getenv("db_host"),
            "DB_USER": os.getenv("db_user"),
            "DB_PASS": os.getenv("db_password"),
            "DB_NAME": os.getenv("db_database"),
            "DB_SCHEMA": os.getenv("db_schema"),
            "DB_PORT": os.getenv("db_port"),
            "DB_TABLE": os.getenv("db_table")
    }
    return database_parameters

def get_logger():
    """
    Configures and returns a custom logger with specific settings for logging 
    informational and warning messages to a file.

    The logger is configured to:
    - Use the name "custom_logger".
    - Log messages with levels `INFO` and `WARNING` only.
    - Write log entries to a file named `pipeline_logger.txt` in the current working directory.
    - Format log messages to include the level, timestamp, and message.

    Returns:
        logging.Logger: A configured logger instance.

    Log File:
        - Location: `pipeline_logger.txt` in the current working directory.
        - Format: `LEVEL: MM/DD/YYYY HH:MM:SS Message`

    Example Log Entry:
        INFO: 01/24/2025 03:45:12 Task completed successfully.
    """
    logger = logging.getLogger("custom_logger")
    logger.setLevel(logging.INFO)
    log_file = os.path.join(os.getcwd(), "pipeline_logger.txt")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    class InfoWarningFilter(logging.Filter):
        def filter(self, record):
            return record.levelno in (logging.INFO, logging.WARNING)
    file_handler.addFilter(InfoWarningFilter())
    return logger

if __name__ == "__main__":
    logs_file = get_logger()
    logs_file.info("Iniciando proceso.")
    DB_PARAMETERS = get_environment_variables()
    logs_file.info("Variables de entorno cargadas.")
    data_files_path, query_files_path, auxiliar_data_files_path, jdbc_path = data_paths()
    logs_file.info("Actualizados path de archivos.")
    SSession = get_SparkSession(jdbc_path)
    db_connection, cursor = get_connection(DB_PARAMETERS)
    logs_file.info("Configurada sesión de Spark.")
    logs_file.info("Conexión establecida con la base de datos: " + str(db_connection))
    execute_sql_files(query_files_path, db_connection, cursor, logs_file)
    _, max_values, min_values, rate_per_destination = get_data(SSession, values_path=auxiliar_data_files_path)
    logs_file.info("Cargados archivos auxiliares para valores máximos, mínimos y tasa de consumo.")
    total_rows_processed, sum_calculated_rate = 0, 0.0
    for file_path in data_files_path[:]:
        data, _, _, _ = get_data(SSession, file_path)
        logs_file.info(f"Procesando archivo: {file_path}")
        total_rows_processed, sum_calculated_rate = process_data(data, max_values, min_values, rate_per_destination, DB_PARAMETERS, total_rows_processed, sum_calculated_rate, logs_file)
        logs_file.info(f"- Total acumulado de filas procesadas: {total_rows_processed}")
        logs_file.info(f"- Total acumulado de 'Tasa Calculada': {sum_calculated_rate}")
        print(f"- Total acumulado de filas procesadas: {total_rows_processed}")
        print(f"- Total acumulado de 'Tasa Calculada': {sum_calculated_rate}")
        print("--------------------------------")
    print("Procesamiento completo:")
    print(f"- Total de filas procesadas: {total_rows_processed}")
    print(f"- Suma total de 'Tasa Calculada': {sum_calculated_rate}")
    logs_file.info("Pipeline finalizado.")
    logs_file.info("--------------------------------")
    get_results(query_files_path[-1], db_connection, cursor, logs_file)
    print("--------------------------------")
    logs_file.info("--------------------------------")
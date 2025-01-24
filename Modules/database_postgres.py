import psycopg2

def execute_sql_files(files_path=[], db_connection=None, cursor=None, logs_file=None):
    """
    Executes a list of SQL script files on a database connection.

    The function performs the following steps:
    1. Sets the search path to the `CONSUMPTIONS` schema.
    2. Truncates the `fact_consumptions` table.
    3. Iterates over the provided SQL files, executes their contents, and commits the changes.
    4. Logs the successful execution of each file.

    Args:
        files_path (list): A list of file paths (str) to the SQL script files to be executed.
        db_connection (object): The database connection object to commit transactions.
        cursor (object): The database cursor object used to execute SQL commands.
        logs_file (logging.Logger): A logger instance to log the execution of SQL files.

    Returns:
        None

    Logs:
        - Logs an informational message for each SQL file executed successfully.

    Notes:
        - Ensure the database connection and cursor are properly initialized before calling this function.
        - The SQL files must contain valid SQL commands that can be executed by the database.
    """
    cursor.execute('SET search_path TO CONSUMPTIONS;')
    cursor.execute('TRUNCATE TABLE fact_consumptions;')
    for file_path in files_path:
        with open(file_path, 'r') as sql_file:
            sql_commands = sql_file.read()
        cursor.execute(sql_commands)
        db_connection.commit()
        logs_file.info("Archivo SQL " + str(file_path) + " ejecutado exitosamente.")
    return None
def get_connection(DB_PARAMETERS):
    """
    Establishes a connection to a PostgreSQL database using the provided database parameters.

    Args:
        DB_PARAMETERS (dict): A dictionary containing the database connection parameters:
            - "DB_HOST" (str): The database server host.
            - "DB_USER" (str): The database user.
            - "DB_PASS" (str): The database user's password.
            - "DB_NAME" (str): The name of the database.
            - "DB_PORT" (str): The port number of the database server.
            - "DB_SCHEMA" (str): The database schema to set as the search path.

    Returns:
        tuple:
            - db_connection (psycopg2.connection): The database connection object.
            - cursor (psycopg2.cursor): The cursor object for executing database queries.

    Example:
        DB_PARAMETERS = {
            "DB_HOST": "localhost",
            "DB_USER": "user",
            "DB_PASS": "password",
            "DB_NAME": "database_name",
            "DB_PORT": "5432",
            "DB_SCHEMA": "schema_name"
        }

        db_connection, cursor = get_connection(DB_PARAMETERS)

    Notes:
        - Ensure that the `psycopg2` library is installed before using this function.
    """
    try:
        db_connection = psycopg2.connect(
            host = DB_PARAMETERS["DB_HOST"],
            user = DB_PARAMETERS["DB_USER"],
            password = DB_PARAMETERS["DB_PASS"],
            database = DB_PARAMETERS["DB_NAME"],
            port = DB_PARAMETERS["DB_PORT"],
            options=f'-c search_path={DB_PARAMETERS["DB_SCHEMA"]}'
        )
        cursor = db_connection.cursor()
        return db_connection, cursor
    except Exception as E:
        return print(E)

def get_results(query_path, db_connection=None, cursor=None, logs_file=None):
    """
    Executes an SQL file from the filesystem and retrieves the query results.

    This function connects to the database, sets the search path to 'CONSUMPTIONS',
    reads and executes the provided SQL file, and then returns the query results.
    It also logs important events to a log file and displays the query results with column names.

    Args:
        query_path (str): Path to the SQL file containing the query to execute.
        db_connection (object, optional): Database connection. If not provided, an active connection is assumed.
        cursor (object, optional): Database cursor used to execute the query. If not provided, an active cursor is assumed.
        logs_file (object, optional): Logger object for logging events. If not provided, no logs are recorded.

    Returns:
        list: Query results as a list of tuples. Each tuple represents a row in the result set.

    Example:
        query_path = 'path/to/query.sql'
        db_connection = connection_to_db
        cursor = db_connection.cursor()
        logs_file = logging.getLogger()

        results = get_results(query_path, db_connection, cursor, logs_file)
    """
    try:
        cursor.execute('SET search_path TO CONSUMPTIONS;')
        with open(query_path, 'r') as sql_file:
            sql_commands = sql_file.read()
            cursor.execute(sql_commands)
            db_connection.commit()
            logs_file.info("Archivo SQL " + str(query_path) + " ejecutado exitosamente.")
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        for row in range(0, len(column_names)):
            logs_file.info(f"- {column_names[row]}: {results[0][row]}")
            print(column_names[row] + " = " + str(results[0][row]))
        return results
    except Exception as e:
        print(e)
        logs_file.error(f"Error al ejecutar el archivo SQL {str(query_path)}: {str(e)}")
        return None
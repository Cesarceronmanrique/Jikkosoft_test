from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower, regexp_replace, round, row_number, trim, when
from pyspark.sql.window import Window

def get_data(SparkSesion, file_path = None, values_path = None):
    """
    Loads data into Spark DataFrames based on the provided file paths.

    This function reads either a text file (when `file_path` is provided) or multiple CSV files 
    containing auxiliary data such as maximum values, minimum values, and rates per destination 
    (when `values_path` is provided).

    Args:
        SparkSesion (pyspark.sql.session.SparkSession): The active Spark session used to read data.
        file_path (str, optional): Path to the text file containing the main dataset. Defaults to None.
        values_path (list, optional): List of paths to CSV files containing auxiliary data. 
            Expected structure:
            - values_path[0]: Path to the file containing maximum values.
            - values_path[1]: Path to the file containing minimum values.
            - values_path[2]: Path to the file containing rates per destination.
            Defaults to None.

    Returns:
        tuple: A tuple containing:
            - data (pyspark.sql.DataFrame or None): The DataFrame for the main dataset when `file_path` is provided. 
              Returns None if `values_path` is provided instead.
            - max_values (pyspark.sql.DataFrame or None): The DataFrame for maximum values when `values_path` is provided. 
              Returns None if `file_path` is provided.
            - min_values (pyspark.sql.DataFrame or None): The DataFrame for minimum values when `values_path` is provided. 
              Returns None if `file_path` is provided.
            - rate_per_destination (pyspark.sql.DataFrame or None): The DataFrame for rates per destination 
              when `values_path` is provided. Returns None if `file_path` is provided.

    Example Usage:
        # Load main dataset
        data, _, _, _ = get_data(SparkSesion, file_path="path/to/dataset.txt")

        # Load auxiliary data
        _, max_values, min_values, rate_per_destination = get_data(
            SparkSesion, values_path=[
                "path/to/max_values.csv",
                "path/to/min_values.csv",
                "path/to/rate_per_destination.csv"
            ]
        )
    """
    if values_path is None:
        data = SparkSesion.read.format("text").option("delimiter", ",").csv(file_path, header=True, inferSchema=True)
        return data, None, None, None
    if file_path is None:
        max_values = SparkSesion.read.option("header", "true").csv(values_path[0])
        max_values = max_values.withColumn("Máximo", regexp_replace(col("Máximo"), '"', '').cast("string")) \
                                .withColumn("Máximo",regexp_replace(col("Máximo"), r'\.', '')) \
                                .withColumn("Máximo",regexp_replace(col("Máximo"), ',', '.').cast("float")) \
                                .withColumn("Máximo", round(col("Máximo"), 2))
        min_values = SparkSesion.read.option("header", "true").csv(values_path[1])
        min_values = min_values.withColumn("Mínimo", regexp_replace(col("Mínimo"), '"', '').cast("string")) \
                                .withColumn("Mínimo",regexp_replace(col("Mínimo"), r'\.', '')) \
                                .withColumn("Mínimo",regexp_replace(col("Mínimo"), ',', '.').cast("float")) \
                                .withColumn("Mínimo", round(col("Mínimo"), 2))
        rate_per_destination = SparkSesion.read.option("header", "true").csv(values_path[2])
        rate_per_destination = rate_per_destination.withColumn(
                                "Tarifa sobre consumo", 
                                round(
                                    regexp_replace(
                                        regexp_replace(col("Tarifa sobre consumo"), "%", ""), 
                                        ",", "."
                                    ).cast("float"),
                                    2
                                )
                            )
        return None, max_values, min_values, rate_per_destination

def get_SparkSession(jdbc_path):
    """
    Initializes and configures a SparkSession for the application.

    This function creates a SparkSession with a specified JDBC driver, sets the application name, 
    and adjusts the log level to reduce unnecessary logging output.

    Args:
        jdbc_path (str): Path to the JDBC driver JAR file required for database connectivity.

    Returns:
        pyspark.sql.session.SparkSession: A configured SparkSession instance.

    Example Usage:
        jdbc_path = "/path/to/postgresql-42.7.5.jar"
        spark = get_SparkSession(jdbc_path)
    """
    SS = SparkSession.builder.appName("JikkoSoft_Data_Pipeline").config("spark.jars", jdbc_path).getOrCreate()
    SS.sparkContext.setLogLevel("ERROR")
    return SS
def map_column_names(Dataframe):
    """
    Renames specific columns in a PySpark DataFrame to standardized names.

    This function maps the original column names in the provided DataFrame to new names, 
    ensuring consistency and alignment with naming conventions (e.g., uppercase with underscores).

    Args:
        Dataframe (pyspark.sql.DataFrame): The input DataFrame containing the original column names.

    Returns:
        pyspark.sql.DataFrame: A new DataFrame with renamed columns.

    Renamed Columns:
        - "id" -> "ID"
        - "año" -> "YEAR"
        - "id sector" -> "SECTOR_ID"
        - "estrato" -> "STRATUM"
        - "consumo" -> "CONSUMPTION"
        - "Tarifa sobre consumo" -> "RATE"
        - "Tasa Calculada" -> "CALCULATED_RATE"

    Example Usage:
        # Input DataFrame
        df = spark.read.csv("path/to/file.csv", header=True)
        
        # Standardize column names
        standardized_df = map_column_names(df)
    """
    Dataframe = Dataframe \
        .withColumnRenamed("id", "ID") \
        .withColumnRenamed("año", "YEAR") \
        .withColumnRenamed("id sector", "SECTOR_ID") \
        .withColumnRenamed("estrato", "STRATUM") \
        .withColumnRenamed("consumo", "CONSUMPTION") \
        .withColumnRenamed("Tarifa sobre consumo", "RATE") \
        .withColumnRenamed("Tasa Calculada", "CALCULATED_RATE")
    return Dataframe
def process_data(data = None, max_values = None, min_values = None, rate_per_destination = None, db_parameters = None, rows_counter = 0, sum_rate = 0.0, logs_file=None, batch_size = 1000):
    """
    Processes the input data in batches, applying various transformations and calculations, and writes the result to a PostgreSQL database.

    This function performs the following tasks:
    - Trims and standardizes column names.
    - Joins the input data with external rate and maximum/minimum values.
    - Calculates the "Tasa calculada" (calculated rate) and applies boundaries based on maximum and minimum values.
    - Transforms the 'destino' (destination) column into sector IDs based on predefined categories.
    - Writes the processed data to a PostgreSQL database.
    - Logs the progress and statistics of the processing.

    Args:
        data (pyspark.sql.DataFrame, optional): The input data to be processed. Defaults to None.
        max_values (pyspark.sql.DataFrame, optional): DataFrame containing the maximum values used for rate capping. Defaults to None.
        min_values (pyspark.sql.DataFrame, optional): DataFrame containing the minimum values used for rate floor. Defaults to None.
        rate_per_destination (pyspark.sql.DataFrame, optional): DataFrame containing rate data per destination. Defaults to None.
        db_parameters (dict, optional): Database connection parameters for PostgreSQL. Defaults to None.
        rows_counter (int, optional): Running total of rows processed. Defaults to 0.
        sum_rate (float, optional): Running total of the sum of calculated rates. Defaults to 0.0.
        logs_file (logging.Logger, optional): A logging file where process statistics are logged. Defaults to None.
        batch_size (int, optional): The number of rows to process in each batch. Defaults to 1000.

    Returns:
        tuple: A tuple containing:
            - rows_counter (int): The total number of rows processed.
            - sum_rate (float): The total sum of the 'CALCULATED_RATE' column.

    Example:
        rows_processed, rate_sum = process_data(data, max_values, min_values, rate_per_destination, db_parameters)
    """
    data = data.withColumn("destino", trim(col("destino")).cast("string"))
    rate_per_destination = rate_per_destination.withColumn("Destino", trim(col("Destino")).cast("string"))
    window_spec = Window.orderBy(lit(1))
    data = data.withColumn("index", row_number().over(window_spec))
    for i in range(0, data.count(), batch_size):
        micro_batch_data = data.filter((data["index"] > i) & (data["index"] <= i + batch_size))
        consumptions = micro_batch_data.alias("bat").join(
            rate_per_destination.alias("rat"),
            (lower(col("bat.destino")) == lower(col("rat.Destino"))) &
            (
                when(col("bat.destino") == "Residencial", col("bat.estrato") == col("rat.Estrato"))
                .otherwise(lit(True))
            ),
            how="left"
        )
        consumptions = consumptions.withColumn(
            "Tasa calculada",
            round(((consumptions["consumo"] * consumptions["Tarifa sobre consumo"]) / 100), 2)
        )
        consumptions = consumptions.drop(col("rat.Destino"), col("rat.Estrato"), col("index"))
        consumptions = consumptions.alias("con").join(
            max_values.alias("max"),
            (col("con.año") == col("max.Año")) & (col("con.destino") == col("max.Destino")),
            how="left"
        )
        consumptions = consumptions.withColumn(
            "Subtotal",
            when(col("Máximo").isNull(), col("Tasa calculada")).otherwise(
            when(col("Tasa calculada") >= col("Máximo"), col("Máximo")).otherwise(col("Tasa calculada")))
        )
        consumptions = consumptions.drop(col("max.Año"), col("max.Destino"), col("max.Máximo"))
        consumptions = consumptions.alias("con").join(
            min_values.alias("min"),
            (col("con.año") == col("min.Año")),
            how="left"
        )
        consumptions = consumptions.withColumn(
            "Tasa Calculada",
            round(
                when(col("Mínimo").isNull(), col("Tasa calculada"))
                .otherwise(
                    when(col("Subtotal") >= col("Mínimo"), col("Subtotal"))
                    .otherwise(col("Mínimo"))
                ),
                2)
        )
        consumptions = consumptions.withColumn(
            "id sector", when(col("destino") == "Comercial", 1)
            .when(col("destino") == "Industrial", 2)
            .when(col("destino") == "Oficial", 3)
            .when(col("destino") == "Especial", 4)
            .when(col("destino") == "Otros", 5)
            .when(col("destino") == "Residencial", 6)
            )
        consumptions = consumptions.drop(col("max.Subtotal"), col("min.Año"), col("min.Mínimo"), col("Subtotal"), col("destino"))
        consumptions = map_column_names(consumptions)
        final_dataframe = consumptions.select(*["ID", "YEAR", "SECTOR_ID", "STRATUM", "CONSUMPTION", "RATE", "CALCULATED_RATE"])
        write_data_to_postgresql(db_parameters, final_dataframe)
        rows_processed = consumptions.count()
        calculated_rate = consumptions.agg({"CALCULATED_RATE": "sum"}).collect()[0][0] or 0
        rows_counter += rows_processed
        sum_rate += calculated_rate
        logs_file.info(f"Se procesará el lote {i}-{i + batch_size}:")
        logs_file.info(f"- Filas procesadas: {rows_counter}")
        logs_file.info(f"- Suma de 'Tasa Calculada': {sum_rate}")
        print("--------------------------------")
        print(f"Lote {i}-{i + batch_size}:")
        print(f"- Filas procesadas: {rows_processed}")
        print(f"- Suma de 'Tasa Calculada': {sum_rate}")
    return rows_counter, sum_rate

def write_data_to_postgresql(DB_PARAMETERS, dataframe):
    """
    Writes the specified DataFrame to a PostgreSQL database.

    This function connects to a PostgreSQL database using the provided connection parameters
    and writes the data from the given DataFrame to the specified table.

    Args:
        DB_PARAMETERS (dict): A dictionary containing the database connection parameters.
            Expected keys:
                - DB_HOST (str): The hostname of the PostgreSQL server.
                - DB_PORT (int): The port number of the PostgreSQL server.
                - DB_NAME (str): The name of the database to connect to.
                - DB_USER (str): The database user for authentication.
                - DB_PASS (str): The password for the database user.
                - DB_SCHEMA (str): The schema in the PostgreSQL database where the table is located.
                - DB_TABLE (str): The table name where the data will be inserted.
        dataframe (pyspark.sql.DataFrame): The Spark DataFrame to be written to the database.
        
    Returns:
        None: This function performs an insert operation and does not return any value.

    Example:
        write_data_to_postgresql(DB_PARAMETERS, my_dataframe)
    """
    try:
        URL = f"jdbc:postgresql://{DB_PARAMETERS['DB_HOST']}:{DB_PARAMETERS['DB_PORT']}/{DB_PARAMETERS['DB_NAME']}"
        dataframe.select("ID", "YEAR", "SECTOR_ID", "STRATUM", "CONSUMPTION", "RATE", "CALCULATED_RATE").write \
            .format("jdbc") \
            .option("url", URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", str(DB_PARAMETERS["DB_SCHEMA"]) + "." + str(DB_PARAMETERS["DB_TABLE"])) \
            .option("user", DB_PARAMETERS["DB_USER"]) \
            .option("password", DB_PARAMETERS["DB_PASS"]) \
            .mode("append") \
            .save()
        print("Datos insertados correctamente.")
    except Exception as e:
        print(f"Error al escribir en la base de datos: {e}")
    return None
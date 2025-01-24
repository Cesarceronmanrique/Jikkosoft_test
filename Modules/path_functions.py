import os

def data_paths():
    """
    Generates and returns the file paths for datasets, SQL scripts, auxiliary data, 
    and a JDBC driver based on the current working directory.

    Returns:
        tuple: A tuple containing:
            - files_path (list): List of full paths to dataset files.
                Example: ["<current_directory>/Data/dataset-1.txt", ...]
            - querys_path (list): List of full paths to SQL script files.
                Example: ["<current_directory>/SQL/create_dim_sectors.sql", ...]
            - values_path (list): List of full paths to auxiliary data files.
                Example: ["<current_directory>/Data/maximos.csv", ...]
            - jdbc_data_path (str): Full path to the JDBC driver file.
                Example: "<current_directory>/postgresql-42.7.5.jar"

    File Details:
        - Dataset Files (files_path):
            "dataset-1.txt", "dataset-2.txt", "dataset-3.txt", "dataset-4.txt", "dataset-5.txt"
        - SQL Script Files (querys_path):
            "create_dim_sectors.sql", "insert_dim_sectors.sql", "create_fact_consumptions.sql"
        - Auxiliary Data Files (values_path):
            "maximos.csv", "minimos.csv", "tarifa_por_destino.csv"
        - JDBC Driver File:
            "postgresql-42.7.5.jar"

    Example:
        files_path, querys_path, values_path, jdbc_data_path = data_paths()
    """
    actual_path = os.getcwd()
    files_names = ["dataset-1.txt","dataset-2.txt","dataset-3.txt","dataset-4.txt","dataset-5.txt"]
    query_names = ["create_dim_sectors.sql", "insert_dim_sectors.sql", "create_fact_consumptions.sql", "get_results.sql"]
    auxiliar_data_names = ["maximos.csv", "minimos.csv", "tarifa_por_destino.csv"]
    jdbc_data_path = os.path.join(actual_path, "postgresql-42.7.5.jar")
    data_path = os.path.join(actual_path, "Data")
    sql_path = os.path.join(actual_path, "SQL")
    files_path, querys_path, values_path = [], [], []
    for file_name in files_names:
        file_path = os.path.join(data_path, file_name)
        files_path.append(file_path)
    for file_name in query_names:
        file_path = os.path.join(sql_path, file_name)
        querys_path.append(file_path)
    for file_name in auxiliar_data_names:
        file_path = os.path.join(data_path, file_name)
        values_path.append(file_path)
    return files_path, querys_path, values_path, jdbc_data_path
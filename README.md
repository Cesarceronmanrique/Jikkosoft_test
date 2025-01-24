# Jikkosoft Technical Test

Este repositorio contiene los archivos necesarios para las pruebas técnicas de Jikkosoft. A continuación, se describen los principales componentes y su propósito.

## Estructura del Repositorio

- **Data/**: Directorio que contiene los datos necesarios para las pruebas.
- **Modules/**: Módulos de Python que encapsulan la lógica y las funciones necesarias para las operaciones del proyecto.
- **SQL/**: Scripts SQL utilizados para realizar consultas y operaciones en la base de datos.
- **.gitignore**: Archivo que define los archivos y directorios que deben ser ignorados por Git.
- **README.md**: Este archivo, que proporciona una descripción general del repositorio.
- **main.py**: Script principal de Python que sirve como punto de entrada para la ejecución del proyecto.
- **pipeline_logger.txt**: Archivo de log generado por el sistema para registrar eventos y errores.
- **postgresql-42.7.5.jar**: Controlador JDBC para conectar aplicaciones Java con bases de datos PostgreSQL.

## Requisitos Previos

Antes de utilizar este proyecto, asegúrate de cumplir con los siguientes requisitos:
1. Python 3.8 o superior instalado.
2. PostgreSQL configurado y funcionando.
3. Dependencias instaladas según lo definido en `requirements.txt` (si corresponde).
4. Java Runtime Environment (JRE) para utilizar el archivo `postgresql-42.7.5.jar`.

## Uso

1. **Configuración de la base de datos:**
   - Utiliza los scripts disponibles en el directorio `SQL/` para configurar las tablas y datos necesarios en tu instancia de PostgreSQL.

2. **Ejecución del proyecto:**
   - Ejecuta el archivo principal `main.py` para iniciar el proceso.

   ```bash
   python main.py

## Resultados

En el archivo `pipeline_logger` se encuentran los resultados de la ejecución por `Microbatches` y los resultados referentes a la consulta a la base de datos. 

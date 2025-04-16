# PySpark Database Schema Generator

This project is a PySpark-based tool that generates JSON schema files for tables in SQL Server, Oracle, or Teradata databases. It reads a JSON configuration file, validates it against a JSON schema, connects to the specified database, retrieves table schemas, applies optional column aliases, and outputs simplified JSON schemas (excluding metadata like `precision`, `scale`, or `length`). The codebase is modular, with separate components for configuration loading, database connections, schema generation, and JSON output handling.

## Features
- **Supported Databases**: SQL Server, Oracle, Teradata.
- **Configuration**: JSON file specifying a single database, connection details, optional schemas/tables, column aliases, and output directory.
- **Validation**: Enforces configuration correctness using an external JSON schema (`config_schema.json`).
- **Schema Output**: Simplified JSON format with table metadata (`full_name`, `schema`, `name`) and columns (`name`, `type`, `nullable`).
- **Modularity**: Codebase split into five Python modules for configuration, connections, schema generation, output handling, and orchestration.
- **Filtering**: Supports schema and table filtering via configuration.
- **Column Aliases**: Allows renaming columns in the output schema.

## File Structure

```
project_directory/
├── schema_generator.py        # Main entry point and orchestration
├── config_loader.py           # Configuration loading and validation
├── database_connection.py     # Database connection logic
├── schema_generator_db.py     # Schema generation logic
├── schema_output_handler.py   # JSON schema output handling
├── config_schema.json         # JSON schema for configuration validation
├── config.json                # User-defined configuration file
├── README.md                  # Project documentation
```


## Component Details

### 1. `schema_generator.py`
- **Purpose**: Entry point that orchestrates the schema generation process.
- **Components**:
  - `main`: Loads configuration, creates database connection and schema generator, and triggers schema generation.
  - Command-line argument parsing for `config.json` and `config_schema.json` paths.
- **Dependencies**: Imports `ConfigLoader`, `DatabaseConnectionFactory`, `SchemaGeneratorFactory`.

### 2. `config_loader.py`
- **Purpose**: Loads and validates the JSON configuration file against `config_schema.json`.
- **Components**:
  - `ConfigLoader`:
    - `_load_schema`: Loads the JSON schema file.
    - `load_config`: Loads and validates the configuration file using `jsonschema`.
- **Dependencies**: `json`, `jsonschema`.

### 3. `database_connection.py`
- **Purpose**: Manages database connections for SQL Server, Oracle, and Teradata.
- **Components**:
  - `DatabaseConnection` (abstract base class): Defines interface for JDBC URL, connection properties, Spark session, and cleanup.
  - `SqlServerConnection`: Implements connection for SQL Server.
  - `OracleConnection`: Implements connection for Oracle.
  - `TeradataConnection`: Implements connection for Teradata.
  - `DatabaseConnectionFactory`: Creates appropriate connection based on database type.
- **Dependencies**: `pyspark.sql.SparkSession`, `abc`.

### 4. `schema_generator_db.py`
- **Purpose**: Generates table schemas from the database.
- **Components**:
  - `DatabaseSchemaGenerator` (abstract base class): Defines methods for retrieving table names, applying aliases, and generating schemas.
  - `SqlServerSchemaGenerator`: Implements schema generation for SQL Server.
  - `OracleSchemaGenerator`: Implements schema generation for Oracle.
  - `TeradataSchemaGenerator`: Implements schema generation for Teradata.
  - `SchemaGeneratorFactory`: Creates appropriate schema generator based on database type.
- **Dependencies**: `typing`, `pyspark.sql.types`, `abc`, `database_connection.DatabaseConnection`, `schema_output_handler.SchemaOutputHandler`.

### 5. `schema_output_handler.py`
- **Purpose**: Converts Spark schemas to simplified JSON and writes them to files.
- **Components**:
  - `SchemaOutputHandler`:
    - `map_spark_type`: Maps Spark data types to simplified names (e.g., `IntegerType` to `integer`).
    - `schema_to_structured_json`: Converts schema to JSON with table metadata and columns.
    - `write_schema_to_file`: Writes JSON schema to a file in the output directory.
- **Dependencies**: `json`, `os`, `pyspark.sql.types`.

### 6. `config_schema.json`
- **Purpose**: Defines the validation rules for `config.json`.
- **Structure**:
  - Requires a single `database` object and `output_dir`.
  - `database` fields: `type` (sqlserver, oracle, teradata), connection parameters (e.g., `server`, `username`), optional `schemas`, `tables`, `column_aliases`.
  - Enforces database-specific requirements (e.g., `port` for Oracle).
  - Validates formats (e.g., `tables` as `schema.table`).

### 7. `config.json`
- **Purpose**: User-defined configuration specifying the database and schema generation settings.
- **Example**:
  ```json
  {
    "database": {
      "type": "sqlserver",
      "server": "localhost",
      "database": "my_db",
      "username": "sa",
      "password": "password123",
      "schemas": ["dbo"],
      "tables": ["dbo.employees", "dbo.departments"],
      "column_aliases": {
        "dbo.employees": {
          "emp_id": "employee_id",
          "emp_name": "full_name"
        },
        "dbo.departments": {
          "dept_id": "department_id"
        }
      }
    },
    "output_dir": "./schemas"
  }
  ```


## Prerequisites
**Python**: 3.8 or higher.

**Dependencies**:
  - `pyspark`: For Spark functionality and JDBC connections.
  - `jsonschema`: For configuration validation.

**JDBC Drivers**:
  - SQL Server: `com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8`
  - Oracle: `com.oracle.database.jdbc:ojdbc8:21.7.0.0` (may require manual download from Oracle's website)
  - Teradata: `com.teradata.jdbc:terajdbc4:17.10.0.1` (may require `tdgssconfig.jar`)

## Build Instructions

**Clone the Repository** (if applicable):
```bash
git clone <repository_url>
cd project_directory
```

**Install Dependencies**:

Install the required Python packages:
```bash
pip install pyspark jsonschema
```

**Prepare JDBC Drivers**:

The tool uses `spark.jars.packages` to download drivers automatically for SQL Server and Teradata.

For Oracle, download `ojdbc8.jar` from Oracle's website and include it manually if needed:
```bash
export SPARK_CLASSPATH=/path/to/ojdbc8.jar
```

For offline environments, download all required JARs and configure Spark manually:
```bash
export SPARK_CLASSPATH=/path/to/mssql-jdbc.jar:/path/to/ojdbc8.jar:/path/to/terajdbc4.jar:/path/to/tdgssconfig.jar
```

#### Set Up Configuration Files:

Create config_schema.json in the project directory with the content provided above.
Create config.json with your database details (see example above).

## Run Instructions

Verify File Structure:
Ensure the following files are in the project directory:
schema_generator.py

config_loader.py

database_connection.py

schema_generator_db.py

schema_output_handler.py

config_schema.json

config.json

Run the Script:
Execute schema_generator.py with default file paths:
bash

python schema_generator.py

Or specify custom paths:
bash

python schema_generator.py --config my_config.json --schema my_schema.json

Expected Output:
The script validates config.json against config_schema.json.

Connects to the specified database.

Generates schemas for the specified tables or schemas.

Applies column aliases if provided.

Writes JSON schema files to output_dir (e.g., ./schemas/dbo_employees_schema.json).

Prints progress and any errors:

Processing database: sqlserver
Schema for table dbo.employees written to ./schemas/dbo_employees_schema.json
Schema for table dbo.departments written to ./schemas/dbo_departments_schema.json

## Validation Examples
The ConfigLoader validates config.json against config_schema.json. Below are common errors:
Missing database:
json

{
  "output_dir": "./schemas"
}

Error: ValueError: Configuration validation failed: 'database' is a required property

Invalid output_dir:
json

{
  "database": {
    "type": "sqlserver",
    "server": "localhost",
    "database": "my_db",
    "username": "sa",
    "password": "password123"
  },
  "output_dir": ""
}

Error: ValueError: Configuration validation failed: '' does not match '.*\\S.*'

Missing type:
json

{
  "database": {
    "server": "localhost",
    "database": "my_db",
    "username": "sa",
    "password": "password123"
  },
  "output_dir": "./schemas"
}

Error: ValueError: Configuration validation failed: 'type' is a required property

Invalid type:
json

{
  "database": {
    "type": "mysql",
    "server": "localhost",
    "database": "my_db",
    "username": "sa",
    "password": "password123"
  },
  "output_dir": "./schemas"
}

Error: ValueError: Configuration validation failed: 'mysql' is not one of ['sqlserver', 'oracle', 'teradata']

Missing required parameter:
json

{
  "database": {
    "type": "sqlserver",
    "server": "localhost",
    "username": "sa",
    "password": "password123"
  },
  "output_dir": "./schemas"
}

Error: ValueError: Configuration validation failed: 'database' is a required property

Invalid tables format:
json

{
  "database": {
    "type": "sqlserver",
    "server": "localhost",
    "database": "my_db",
    "username": "sa",
    "password": "password123",
    "tables": ["dbo.employees", "employees"]
  },
  "output_dir": "./schemas"
}

Error: ValueError: Configuration validation failed: 'employees' does not match '^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$'

Invalid schema file:
If config_schema.json is missing or malformed:
Error: ValueError: Failed to read or parse schema file config_schema.json: ...

## Notes
Modularity:
The codebase is split into five Python files, each handling a specific responsibility: configuration, connections, schema generation, output handling, and orchestration.

This improves maintainability and scalability.

JSON Schema Validation:
Uses jsonschema for robust validation with clear error messages.

Enforces a single database, correct type, required parameters, and valid formats.

Simplified Output:
Excludes metadata (e.g., precision, scale, length), including only name, type, and nullable for columns.

Security:
Store sensitive information (e.g., passwords) securely, such as in environment variables or a secrets manager, for production use.

Extensibility:
To support new databases:
Update config_schema.json with new type and requirements.

Add a new connection class in database_connection.py.

Add a new schema generator class in schema_generator_db.py.

JDBC Drivers:
Ensure driver compatibility with your database versions.

For offline environments, include JARs manually in the Spark configuration.

Oracle:
If using SID instead of service name, modify the JDBC URL in OracleConnection:
python

return f"jdbc:oracle:thin:@//{self.server}:{self.port}:{self.service_name}"

Teradata:
Ensure the user has access to DBC.TablesV for table name queries.

## Troubleshooting
Connection Errors:
Verify database credentials and network accessibility.

Ensure JDBC drivers are available and compatible.

Validation Errors:
Check config.json against config_schema.json requirements.

Ensure tables are in schema.table format.

Spark Errors:
Confirm pyspark is installed and configured.

Check SPARK_CLASSPATH for manual JAR inclusion.

File Not Found:
Ensure config.json and config_schema.json are in the project directory or specify correct paths.


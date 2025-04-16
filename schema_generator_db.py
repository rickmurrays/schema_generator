from typing import List, Optional, Dict
from pyspark.sql.types import StructType, StructField
from abc import ABC, abstractmethod
from database_connection import DatabaseConnection
from schema_output_handler import SchemaOutputHandler

class DatabaseSchemaGenerator(ABC):
    """Abstract base class for database schema generation."""
    
    def __init__(self, connection: DatabaseConnection):
        """
        Initialize the schema generator with a database connection.
        
        Args:
            connection (DatabaseConnection): Database connection instance
        """
        self.connection = connection
        self.spark = connection.get_spark_session("SchemaGenerator")

    @abstractmethod
    def get_table_names_query(self, schemas: Optional[List[str]] = None) -> str:
        """
        Get the query to retrieve table names.
        
        Args:
            schemas (Optional[List[str]]): List of schemas to filter
            
        Returns:
            str: SQL query to retrieve table names
        """
        pass

    def get_table_names(self, schemas: Optional[List[str]] = None, tables: Optional[List[str]] = None) -> List[str]:
        """
        Retrieve table names from the database, filtered by schemas and tables if provided.
        
        Args:
            schemas (Optional[List[str]]): List of schemas to filter
            tables (Optional[List[str]]): List of fully qualified table names to filter
            
        Returns:
            List[str]: List of fully qualified table names
        """
        if tables:
            # If specific tables are provided, use them directly
            return tables
        
        # Otherwise, query the database for table names, filtered by schemas if provided
        query = self.get_table_names_query(schemas)
        df = (self.spark.read
              .format("jdbc")
              .option("url", self.connection.get_jdbc_url())
              .option("query", query)
              .options(**self.connection.get_connection_properties())
              .load())
        
        return [row[0] for row in df.collect()]

    def apply_column_aliases(self, schema: StructType, table_name: str, column_aliases: Optional[Dict[str, Dict[str, str]]] = None) -> StructType:
        """
        Apply column aliases to the schema.
        
        Args:
            schema (StructType): Original Spark schema
            table_name (str): Fully qualified table name
            column_aliases (Optional[Dict[str, Dict[str, str]]]): Dictionary of table-specific column aliases
            
        Returns:
            StructType: Schema with applied aliases
        """
        if not column_aliases or table_name not in column_aliases:
            return schema
        
        aliases = column_aliases.get(table_name, {})
        new_fields = []
        for field in schema.fields:
            # Use the alias if it exists, otherwise keep the original name
            new_name = aliases.get(field.name, field.name)
            new_fields.append(StructField(
                name=new_name,
                dataType=field.dataType,
                nullable=field.nullable,
                metadata=field.metadata
            ))
        
        return StructType(new_fields)

    def get_table_schema(self, table_name: str, column_aliases: Optional[Dict[str, Dict[str, str]]] = None) -> StructType:
        """
        Get schema for a specific table with optional column aliases.
        
        Args:
            table_name (str): Fully qualified table name (schema.table)
            column_aliases (Optional[Dict[str, Dict[str, str]]]): Dictionary of table-specific column aliases
            
        Returns:
            StructType: Table schema with aliases applied
        """
        df = (self.spark.read
              .format("jdbc")
              .option("url", self.connection.get_jdbc_url())
              .option("dbtable", table_name)
              .options(**self.connection.get_connection_properties())
              .load())
        
        schema = df.schema
        return self.apply_column_aliases(schema, table_name, column_aliases)

    def generate_all_schemas(self, output_dir: str, schemas: Optional[List[str]] = None, tables: Optional[List[str]] = None, column_aliases: Optional[Dict[str, Dict[str, str]]] = None):
        """
        Generate schemas for specified tables and write to JSON files using SchemaOutputHandler.
        
        Args:
            output_dir (str): Directory to store JSON schema files
            schemas (Optional[List[str]]): List of schemas to process
            tables (Optional[List[str]]): List of fully qualified table names to process
            column_aliases (Optional[Dict[str, Dict[str, str]]]): Dictionary of table-specific column aliases
        """
        output_handler = SchemaOutputHandler()
        try:
            table_names = self.get_table_names(schemas, tables)
            
            for table_name in table_names:
                try:
                    schema = self.get_table_schema(table_name, column_aliases)
                    schema_dict = output_handler.schema_to_structured_json(schema, table_name)
                    output_handler.write_schema_to_file(schema_dict, table_name, output_dir)
                except Exception as e:
                    print(f"Error processing table {table_name}: {str(e)}")
                    
        except Exception as e:
            print(f"Error retrieving table names: {str(e)}")
        finally:
            self.connection.close()

class SqlServerSchemaGenerator(DatabaseSchemaGenerator):
    def get_table_names_query(self, schemas: Optional[List[str]] = None) -> str:
        if schemas:
            schema_filter = " OR ".join([f"TABLE_SCHEMA = '{schema}'" for schema in schemas])
            return f"""
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' AND ({schema_filter})
            """
        return """
        SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        """

class OracleSchemaGenerator(DatabaseSchemaGenerator):
    def get_table_names_query(self, schemas: Optional[List[str]] = None) -> str:
        if schemas:
            schema_filter = " OR ".join([f"OWNER = '{schema}'" for schema in schemas])
            return f"""
            SELECT OWNER || '.' || TABLE_NAME AS TABLE_NAME
            FROM ALL_TABLES
            WHERE ({schema_filter})
            """
        return """
        SELECT OWNER || '.' || TABLE_NAME AS TABLE_NAME
        FROM USER_TABLES
        """

class TeradataSchemaGenerator(DatabaseSchemaGenerator):
    def __init__(self, connection: DatabaseConnection, database: str):
        super().__init__(connection)
        self.database = database
    
    def get_table_names_query(self, schemas: Optional[List[str]] = None) -> str:
        if schemas:
            schema_filter = " OR ".join([f"DatabaseName = '{schema}'" for schema in schemas])
            return f"""
            SELECT DatabaseName || '.' || TableName AS TableName
            FROM DBC.TablesV 
            WHERE TableKind = 'T' AND ({schema_filter})
            """
        return f"""
        SELECT DatabaseName || '.' || TableName AS TableName
        FROM DBC.TablesV 
        WHERE TableKind = 'T' AND DatabaseName = '{self.database}'
        """

# --- Schema Generator Factory ---

class SchemaGeneratorFactory:
    """Factory for creating database schema generators."""
    
    @staticmethod
    def create_generator(db_type: str, connection: DatabaseConnection, database: Optional[str] = None) -> DatabaseSchemaGenerator:
        """
        Create a schema generator based on the database type.
        
        Args:
            db_type (str): Database type ('sqlserver', 'oracle', 'teradata')
            connection (DatabaseConnection): Database connection instance
            database (Optional[str]): Database name (required for Teradata)
            
        Returns:
            DatabaseSchemaGenerator: Schema generator instance
        """
        db_type = db_type.lower()
        if db_type == "sqlserver":
            return SqlServerSchemaGenerator(connection)
        elif db_type == "oracle":
            return OracleSchemaGenerator(connection)
        elif db_type == "teradata":
            if not database:
                raise ValueError("Database name required for Teradata schema generator")
            return TeradataSchemaGenerator(connection, database)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

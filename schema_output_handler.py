import json
import os
from pyspark.sql.types import StructType, IntegerType, LongType, FloatType, DoubleType, DecimalType, StringType, BooleanType, TimestampType, DateType, VarcharType, CharType

class SchemaOutputHandler:
    """Handles conversion of schemas to structured JSON and writing to files."""
    
    @staticmethod
    def map_spark_type(data_type) -> str:
        """
        Map Spark data type to a simplified type name.
        
        Args:
            data_type: Spark data type
            
        Returns:
            str: Simplified type name
        """
        if isinstance(data_type, IntegerType):
            return "integer"
        elif isinstance(data_type, LongType):
            return "bigint"
        elif isinstance(data_type, FloatType):
            return "float"
        elif isinstance(data_type, DoubleType):
            return "double"
        elif isinstance(data_type, DecimalType):
            return "decimal"
        elif isinstance(data_type, StringType):
            return "string"
        elif isinstance(data_type, (VarcharType, CharType)):
            return "varchar"
        elif isinstance(data_type, BooleanType):
            return "boolean"
        elif isinstance(data_type, TimestampType):
            return "timestamp"
        elif isinstance(data_type, DateType):
            return "date"
        else:
            return str(data_type.simpleString())

    @staticmethod
    def schema_to_structured_json(schema: StructType, table_name: str) -> dict:
        """
        Convert Spark schema to a structured JSON format without additional metadata.
        
        Args:
            schema (StructType): Spark schema
            table_name (str): Fully qualified table name (schema.table)
            
        Returns:
            dict: Structured JSON representation
        """
        schema_name, table_only = table_name.split(".", 1) if "." in table_name else ("", table_name)
        
        columns = []
        for field in schema.fields:
            col_info = {
                "name": field.name,
                "type": SchemaOutputHandler.map_spark_type(field.dataType),
                "nullable": field.nullable
            }
            columns.append(col_info)
        
        return {
            "table": {
                "full_name": table_name,
                "schema": schema_name,
                "name": table_only
            },
            "columns": columns
        }

    @staticmethod
    def write_schema_to_file(schema_dict: dict, table_name: str, output_dir: str):
        """
        Write schema to JSON file.
        
        Args:
            schema_dict (dict): Schema dictionary
            table_name (str): Fully qualified table name
            output_dir (str): Output directory path
        """
        os.makedirs(output_dir, exist_ok=True)
        # Replace dots with underscores for filename to avoid path issues
        safe_table_name = table_name.replace('.', '_')
        output_path = os.path.join(output_dir, f"{safe_table_name}_schema.json")
        
        with open(output_path, 'w') as f:
            json.dump(schema_dict, f, indent=2)
        
        print(f"Schema for table {table_name} written to {output_path}")

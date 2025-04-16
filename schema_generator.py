from config_loader import ConfigLoader
from database_connection import DatabaseConnectionFactory
from schema_generator_db import SchemaGeneratorFactory

# --- Main Function ---

def main(config_file: str = "config.json", schema_file: str = "config_schema.json"):
    """
    Main function to process database schemas based on JSON configuration.
    
    Args:
        config_file (str): Path to the JSON configuration file
        schema_file (str): Path to the JSON schema file
    """
    config = ConfigLoader.load_config(config_file, schema_file)
    output_dir = config.get("output_dir")
    db_config = config.get("database")
    
    try:
        db_type = db_config.get("type", "").lower()
        schemas = db_config.get("schemas")
        tables = db_config.get("tables")
        column_aliases = db_config.get("column_aliases")
        print(f"Processing database: {db_type}")
        
        # Create connection
        connection = DatabaseConnectionFactory.create_connection(db_config)
        
        # Create schema generator
        generator = SchemaGeneratorFactory.create_generator(
            db_type=db_type,
            connection=connection,
            database=db_config.get("database")
        )
        
        # Generate schemas
        generator.generate_all_schemas(output_dir, schemas, tables, column_aliases)
        
    except Exception as e:
        print(f"Error processing database {db_type}: {str(e)}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate database schemas to JSON files")
    parser.add_argument("--config", default="config.json", help="Path to JSON configuration file")
    parser.add_argument("--schema", default="config_schema.json", help="Path to JSON schema file")
    args = parser.parse_args()
    main(args.config, args.schema)

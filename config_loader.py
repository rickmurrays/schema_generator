import json
from jsonschema import validate, ValidationError

class ConfigLoader:
    """Handles loading and validating JSON configuration files using jsonschema."""
    
    @staticmethod
    def _load_schema(schema_file: str = "config_schema.json") -> dict:
        """
        Load JSON schema from an external file.
        
        Args:
            schema_file (str): Path to the JSON schema file
            
        Returns:
            dict: JSON schema dictionary
            
        Raises:
            ValueError: If the schema file cannot be read or parsed
        """
        try:
            with open(schema_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise ValueError(f"Failed to read or parse schema file {schema_file}: {str(e)}")

    @staticmethod
    def load_config(config_file: str, schema_file: str = "config_schema.json") -> dict:
        """
        Load and validate configuration from a JSON file using an external JSON schema.
        
        Args:
            config_file (str): Path to the JSON configuration file
            schema_file (str): Path to the JSON schema file
            
        Returns:
            dict: Validated configuration dictionary
            
        Raises:
            ValueError: If the configuration or schema file is invalid or improperly formatted
        """
        # Load configuration
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
        except Exception as e:
            raise ValueError(f"Failed to read or parse configuration file {config_file}: {str(e)}")

        # Load schema
        schema = ConfigLoader._load_schema(schema_file)

        # Validate configuration against schema
        try:
            validate(instance=config, schema=schema)
        except ValidationError as e:
            raise ValueError(f"Configuration validation failed: {str(e.message)}")

        return config

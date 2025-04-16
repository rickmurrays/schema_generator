from pyspark.sql import SparkSession
from abc import ABC, abstractmethod

# --- Database Connection Factory ---

class DatabaseConnection(ABC):
    """Abstract base class for database connections."""
    
    @abstractmethod
    def get_jdbc_url(self) -> str:
        pass
    
    @abstractmethod
    def get_connection_properties(self) -> dict:
        pass
    
    @abstractmethod
    def get_spark_session(self, app_name: str) -> SparkSession:
        pass
    
    @abstractmethod
    def close(self):
        pass

class SqlServerConnection(DatabaseConnection):
    def __init__(self, server: str, database: str, username: str, password: str):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.spark = None
    
    def get_jdbc_url(self) -> str:
        return f"jdbc:sqlserver://{self.server};databaseName={self.database};encrypt=true;trustServerCertificate=true"
    
    def get_connection_properties(self) -> dict:
        return {
            "user": self.username,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
    
    def get_spark_session(self, app_name: str) -> SparkSession:
        if not self.spark:
            self.spark = (SparkSession.builder
                         .appName(app_name)
                         .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8")
                         .getOrCreate())
        return self.spark
    
    def close(self):
        if self.spark:
            self.spark.stop()
            self.spark = None

class OracleConnection(DatabaseConnection):
    def __init__(self, server: str, port: str, service_name: str, username: str, password: str):
        self.server = server
        self.port = port
        self.service_name = service_name
        self.username = username
        self.password = password
        self.spark = None
    
    def get_jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@{self.server}:{self.port}/{self.service_name}"
    
    def get_connection_properties(self) -> dict:
        return {
            "user": self.username,
            "password": self.password,
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
    
    def get_spark_session(self, app_name: str) -> SparkSession:
        if not self.spark:
            self.spark = (SparkSession.builder
                         .appName(app_name)
                         .config("spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:21.7.0.0")
                         .getOrCreate())
        return self.spark
    
    def close(self):
        if self.spark:
            self.spark.stop()
            self.spark = None

class TeradataConnection(DatabaseConnection):
    def __init__(self, server: str, database: str, username: str, password: str):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.spark = None
    
    def get_jdbc_url(self) -> str:
        return f"jdbc:teradata://{self.server}/DATABASE={self.database}"
    
    def get_connection_properties(self) -> dict:
        return {
            "user": self.username,
            "password": self.password,
            "driver": "com.teradata.jdbc.TeraDriver"
        }
    
    def get_spark_session(self, app_name: str) -> SparkSession:
        if not self.spark:
            self.spark = (SparkSession.builder
                         .appName(app_name)
                         .config("spark.jars.packages", "com.teradata.jdbc:terajdbc4:17.10.0.1")
                         .getOrCreate())
        return self.spark
    
    def close(self):
        if self.spark:
            self.spark.stop()
            self.spark = None

class DatabaseConnectionFactory:
    """Factory for creating database connections."""
    
    @staticmethod
    def create_connection(db_config: dict) -> DatabaseConnection:
        """
        Create a database connection based on the database configuration.
        
        Args:
            db_config (dict): Database configuration dictionary
            
        Returns:
            DatabaseConnection: Database connection instance
        """
        db_type = db_config.get("type", "").lower()
        if db_type == "sqlserver":
            return SqlServerConnection(
                server=db_config.get("server"),
                database=db_config.get("database"),
                username=db_config.get("username"),
                password=db_config.get("password")
            )
        elif db_type == "oracle":
            return OracleConnection(
                server=db_config.get("server"),
                port=db_config.get("port"),
                service_name=db_config.get("service_name"),
                username=db_config.get("username"),
                password=db_config.get("password")
            )
        elif db_type == "teradata":
            return TeradataConnection(
                server=db_config.get("server"),
                database=db_config.get("database"),
                username=db_config.get("username"),
                password=db_config.get("password")
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

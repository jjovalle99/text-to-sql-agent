from pydantic import BaseModel, SecretStr


class DatabaseConfig(BaseModel):
    host: str
    database: str
    user: str
    password: SecretStr
    port: int


class PostgresDatabaseConfig(DatabaseConfig):
    schema_name: str

    @property
    def connection_string(self) -> str:
        return (
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password.get_secret_value()} "
            f"host={self.host} "
            f"port={self.port} "
            f"options='-c search_path={self.schema_name},public'"
        )

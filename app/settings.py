from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Payment Service configuration settings."""
    
    DB_HOST: str = "payment-db"
    DB_PORT: int = 5432
    DB_NAME: str = "payment_db"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    
    # Backward compatibility: if DATABASE_URL is provided, it takes precedence
    # DATABASE_URL: str | None = None
    
    ORDER_API_URL: str = "http://order-api:8000"
    
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @computed_field
    @property
    def database_url(self) -> str:
        """Construct database URL from individual parameters or use provided URL."""
        # if self.DATABASE_URL:
        #     return self.DATABASE_URL
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()

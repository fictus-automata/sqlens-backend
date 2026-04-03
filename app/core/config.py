from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Centralized configuration.

    For this MVP, we keep it intentionally small and env-driven.
    """

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/altimate"
    log_level: str = "INFO"
    request_id_header: str = "X-Request-ID"
    sqlglot_dialect: str = "postgres"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()


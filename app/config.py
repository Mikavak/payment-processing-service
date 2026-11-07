from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    rabbitmq_url: str
    api_key: SecretStr = Field(min_length=16)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    retry_base_delay: float = Field(default=1.0, gt=0)
    webhook_timeout: float = Field(default=5.0, gt=0, le=30)
    log_level: str = "INFO"

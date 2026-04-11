from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    groq_api_key: str
    telegram_api_id: int
    telegram_api_hash: str
    telegram_session_string: str = ""
    database_url: str = "sqlite:///./moex_assistant.db"
    frontend_url: str = "http://localhost:3000"
    secret_key: str = "changeme"

    class Config:
        env_file = ".env"


settings = Settings()

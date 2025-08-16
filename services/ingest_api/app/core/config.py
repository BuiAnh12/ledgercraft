import os
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv(".env")

class Setting(BaseModel):
    app_name: str = "LedgeCraft Ingest API"
    debug: bool = True
    
    oltp_user: str = os.getenv("OLTP_USER", "app")
    oltp_password: str = os.getenv("OLTP_PASSWORD", "app_password")
    oltp_db: str = os.getenv("OLTP_DB", "ledgercraft_oltp")
    oltp_port: int = int(os.getenv("OLTP_PORT", "5432"))
    oltp_host: str = os.getenv("OLTP_HOST", "localhost")
    
    @property
    def database_url(self) -> str:
        return f"postgresql+psycopg://{self.oltp_user}:{self.oltp_password}@{self.oltp_host}:{self.oltp_port}/{self.oltp_db}"

settings = Setting()
import os
from dotenv import load_dotenv

# Charge les variables d'environnement depuis le fichier .env s'il existe
load_dotenv()

DB_CONFIG_1 = {
    "user": os.getenv("DB_USER_1", "api"),
    "password": os.getenv("DB_PASSWORD_1", "api"),
    "dsn": os.getenv("DB_DSN_1", "localhost/ORCL"),
    "schemas": ["CAS"]
}

DB_CONFIG_2 = {
    "user": os.getenv("DB_USER_2", "INTEGRATEUR"),
    "password": os.getenv("DB_PASSWORD_2", "integrateur"),
    "dsn": os.getenv("DB_DSN_2", "localhost/ORCL"),
    "schemas": ["CMGP", "PHILEA"] # "SICDA", 
}

DB_CONFIG_CRM = {
    "user": os.getenv("DB_USER_CRM", "qlik"),
    "password": os.getenv("DB_PASSWORD_CRM", "qlik"),
    "dsn": os.getenv("DB_DSN_CRM", "localhost/ORCL"),
    "central_schema": "CRM"
}

SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "15"))

# Retry settings
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "10"))

SOURCE_TABLE_VIEW = "XIMPAYE"
TARGET_TABLE_CONSO = "XIMPAYE_CONSO"

# Path to the JSON monitoring file
MONITORING_FILE = "sync_monitoring.json"

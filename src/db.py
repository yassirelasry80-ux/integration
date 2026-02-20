import oracledb
from sqlalchemy import create_engine, text
import pandas as pd
from typing import Dict, Any, List, Tuple
from contextlib import contextmanager
from .logger import update_monitoring, logger

# On force l'initialisation du client Oracle (mode Thick) si nécessaire
# oracledb.init_oracle_client() permet d'activer le mode Thick si des features avancées le requièrent.
try:
    oracledb.init_oracle_client()
except Exception as e:
    logger.warning(f"Could not init thick client. Defaulting to thin client. Error: {e}")

@contextmanager
def get_connection(db_config: Dict[str, str]):
    """
    Gestionnaire de contexte pour obtenir et fermer une connexion `oracledb`.
    """
    connection = None
    try:
        connection = oracledb.connect(
            user=db_config["user"],
            password=db_config["password"],
            dsn=db_config["dsn"]
        )
        yield connection
    except Exception as e:
        logger.error(f"Failed to connect to Oracle {db_config.get('dsn')} with user {db_config.get('user')}: {e}")
        raise e
    finally:
        if connection:
            connection.close()

def get_engine(db_config: Dict[str, str]):
    """
    Retourne un moteur SQLAlchemy pour la connexion.
    Utilise le format d'URL Oracle: oracle+oracledb://user:password@dsn
    """
    user = db_config["user"]
    password = db_config["password"]
    dsn = db_config["dsn"]
    
    # Construction de l'URL SQLAlchemy
    connection_string = f"oracle+oracledb://{user}:{password}@{dsn}"
    engine = create_engine(connection_string)
    return engine

def execute_select_to_df(db_config: Dict[str, str], query: str, params: tuple = None) -> pd.DataFrame:
    """
    Exécute une requête et retourne les données sous forme de DataFrame pandas.
    Utilise SQLAlchemy pour éviter l'avertissement/erreur pandas.
    """
    engine = get_engine(db_config)
    try:
        with engine.connect() as conn:
            # Prepare params dict if using named binding or positional
            # For exact raw text wrapping:
            stmt = text(query)
            # if params exist, we need to bind them. 
            # Oracledb direct expects positional tuple like (val,), 
            # SQLAlchemy text expects named dicts if it has :name, 
            # or sequence if mapped properly.
            # Easiest way with pandas read_sql and raw driver queries is to pass the raw string and raw params,
            # or map the tuple to the expected bind format.
            if params:
                # If the query uses :1, :2 syntax, passing a list/tuple directly to pandas might fail or succeed depending on SQLAlchemy dialect.
                # However, since the current queries often use :1, SQLAlchemy might complain.
                # Let's use the DBAPI directly via pandas but wrapped in an SQLAlchemy connection if needed, 
                # OR we just map the params correctly.
                # In many of the project queries, there are no params in execute_select_to_df 
                # except in delta dispatch / integrity, e.g. WHERE SYNC_DATE > :1
                pass
            
            # Since Pandas just wants a connectable, we pass engine.
            # Note: Pandas read_sql supports passing 'params' as list/tuple to the raw DBAPI underneath.
            df = pd.read_sql(query, engine, params=params)
            return df
    except Exception as e:
        logger.error(f"Erreur d'exécution de la requête avec SQLAlchemy: {e}")
        raise e
    finally:
        engine.dispose()

def execute_batch(db_config: Dict[str, str], query: str, data: List[tuple], chunk_size: int = 1000):
    """
    Exécute un INSERT ou UPDATE en batch (executemany).
    """
    if not data:
        return
    with get_connection(db_config) as conn:
        with conn.cursor() as cursor:
            # Oracle executemany optimise les insertions de masse
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                cursor.executemany(query, chunk)
            conn.commit()

def fetch_scalar(db_config: Dict[str, str], query: str, params: tuple = None) -> Any:
    """
    Exécute une requête qui retourne une seule valeur (ex: COUNT, MAX(date)).
    """
    with get_connection(db_config) as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else None

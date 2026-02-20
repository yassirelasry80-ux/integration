import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import DB_CONFIG_1, DB_CONFIG_2, SOURCE_TABLE_VIEW
from .db import execute_select_to_df
from .logger import update_monitoring, logger

def extract_schema_data(db_config: dict, schema: str) -> pd.DataFrame:
    """
    Extrait les données de la vue SOURCE_TABLE_VIEW pour un schéma donné.
    """
    start_time = time.time()
    try:
        # Assurer que la connexion a les droits sur le schéma, ou préfixer avec schema.
        # Ici la table est accessible globalement via view_name ou schema.view_name
        query = f"SELECT * FROM {schema}.{SOURCE_TABLE_VIEW}"
        logger.info(f"Éxecution de l'extraction pour le schéma: {schema}")
        
        df = execute_select_to_df(db_config, query)
        
        # 1. Normalisation en MAJUSCULES
        df.columns = df.columns.str.upper()
        
        # 2. Correction BRP_0 -> BPR_0
        if "BRP_0" in df.columns:
            df.rename(columns={"BRP_0": "BPR_0"}, inplace=True)
            
        # 3. Retirer l'ancien DOSSIER_0 s'il existe et ajouter la vraie source
        if "DOSSIER_0" in df.columns:
            df.drop(columns=["DOSSIER_0"], inplace=True)
            
        df["DOSSIER_0"] = schema
        
        duration = round(time.time() - start_time, 2)
        rows = len(df)
        
        update_monitoring(
            stage="extraction",
            step_name=schema,
            status="SUCCESS",
            metrics={"rows_extracted": rows, "duration_seconds": duration},
            message="Extraction complétée."
        )
        return df
        
    except Exception as e:
        duration = round(time.time() - start_time, 2)
        update_monitoring(
            stage="extraction",
            step_name=schema,
            status="FAILURE",
            metrics={"duration_seconds": duration},
            message=str(e)
        )
        logger.error(f"Erreur d'extraction pour le schéma {schema}: {e}")
        return pd.DataFrame() # Retourne df vide en cas d'erreur


def run_parallel_extraction() -> pd.DataFrame:
    """
    Extrait simultanément toutes les données depuis DB_CONFIG_1 et DB_CONFIG_2
    et les concatène dans un seul DataFrame en mémoire.
    """
    targets = []
    # Préparer les cibles
    for schema in DB_CONFIG_1["schemas"]:
        targets.append((DB_CONFIG_1, schema))
        
    for schema in DB_CONFIG_2["schemas"]:
        targets.append((DB_CONFIG_2, schema))
        
    results = []
    
    with ThreadPoolExecutor(max_workers=min(len(targets), 10)) as executor:
        future_to_schema = {
            executor.submit(extract_schema_data, config, schema): schema 
            for config, schema in targets
        }
        
        for future in as_completed(future_to_schema):
            df = future.result()
            
            if not df.empty:
                results.append(df)
                
    if results:
        global_df = pd.concat(results, ignore_index=True)
        return global_df
    else:
        return pd.DataFrame()

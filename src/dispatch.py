import pandas as pd
import time
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import DB_CONFIG_1, DB_CONFIG_2, DB_CONFIG_CRM, TARGET_TABLE_CONSO
from .db import fetch_scalar, execute_select_to_df, execute_batch
from .logger import update_monitoring, logger

def get_target_schemas() -> List[Tuple[dict, str]]:
    """Retourne la liste complète des schémas cibles et leur configuration associée."""
    targets = []
    for schema in DB_CONFIG_1["schemas"]:
        targets.append((DB_CONFIG_1, schema))
    for schema in DB_CONFIG_2["schemas"]:
        targets.append((DB_CONFIG_2, schema))
    return targets

def dispatch_initial(db_config: dict, schema: str):
    """Effectue un dispatch initial : insère tout le CRM dans la cible locale."""
    start_time = time.time()
    try:
        logger.info(f"Dispatching INITIAL vers {schema}")
        # Récupère tout le CRM
        crm_schema = DB_CONFIG_CRM["central_schema"]
        # Important: récupérer les mêmes colonnes pour l'insertion
        query = f"SELECT * FROM {crm_schema}.{TARGET_TABLE_CONSO}"
        df_crm = execute_select_to_df(DB_CONFIG_CRM, query)
        
        if df_crm.empty:
            logger.info(f"Rien à insérer pour {schema} (CRM vide).")
            update_monitoring("dispatching", schema, "SUCCESS", {"duration_seconds": round(time.time() - start_time, 2)}, "CRM vide.")
            return

        # Insertion brute (on suppose que les dates gérées par pandas seront conformes)
        columns = ", ".join(df_crm.columns)
        bind_vars = ", ".join([f":{i+1}" for i in range(len(df_crm.columns))])
        insert_query = f"INSERT INTO {schema}.{TARGET_TABLE_CONSO} ({columns}) VALUES ({bind_vars})"
        
        data_to_insert = [tuple(x) for x in df_crm.to_numpy()]
        execute_batch(db_config, insert_query, data_to_insert)
        
        dur = round(time.time() - start_time, 2)
        update_monitoring("dispatching", schema, "SUCCESS", {"inserts": len(data_to_insert), "duration_seconds": dur}, "Initial dispatch complété.")
    except Exception as e:
        dur = round(time.time() - start_time, 2)
        update_monitoring("dispatching", schema, "FAILURE", {"duration_seconds": dur}, f"Erreur initial dispatch: {e}")
        logger.error(f"Dispatch initial a échoué pour {schema}: {e}")

def dispatch_delta(db_config: dict, schema: str, delta_df: pd.DataFrame):
    """Effectue un Upsert du delta global vers la cible locale."""
    start_time = time.time()
    if delta_df.empty:
        update_monitoring("dispatching", schema, "SUCCESS", {"duration_seconds": 0}, "Delta vide.")
        return
        
    try:
        logger.info(f"Dispatching DIFFERENTIEL vers {schema} ({len(delta_df)} lignes)")
        
        # Récupérer l'existant local pour optimiser l'Upsert
        query_local = f"SELECT NUM_0, DOSSIER_0 FROM {schema}.{TARGET_TABLE_CONSO}"
        local_keys_df = execute_select_to_df(db_config, query_local)
        
        local_keys = set()
        if not local_keys_df.empty:
            local_keys = set(zip(local_keys_df['NUM_0'], local_keys_df['DOSSIER_0']))
            
        insert_data = []
        update_data = []
        
        delta_dict = delta_df.to_dict(orient='index')
        for idx, row in delta_df.iterrows():
            k = (row['NUM_0'], row['DOSSIER_0'])
            if k in local_keys:
                # La ligne existe localement, on met à jour uniquement MNTREG_0 et SYNC_DATE
                update_data.append((row['MNTREG_0'], row['SYNC_DATE'], row['NUM_0'], row['DOSSIER_0']))
            else:
                # N'existe pas: on insère la ligne complète
                insert_data.append(tuple(row))
                
        # Exécuter les opérations
        if insert_data:
            columns = ", ".join(delta_df.columns)
            bind_vars = ", ".join([f":{i+1}" for i in range(len(delta_df.columns))])
            insert_query = f"INSERT INTO {schema}.{TARGET_TABLE_CONSO} ({columns}) VALUES ({bind_vars})"
            execute_batch(db_config, insert_query, insert_data)
            
        if update_data:
            update_query = f"UPDATE {schema}.{TARGET_TABLE_CONSO} SET MNTREG_0 = :1, SYNC_DATE = :2 WHERE NUM_0 = :3 AND DOSSIER_0 = :4"
            execute_batch(db_config, update_query, update_data)
            
        dur = round(time.time() - start_time, 2)
        update_monitoring("dispatching", schema, "SUCCESS", {"inserts": len(insert_data), "updates": len(update_data), "duration_seconds": dur}, "Delta upsert complété.")
    except Exception as e:
        dur = round(time.time() - start_time, 2)
        update_monitoring("dispatching", schema, "FAILURE", {"duration_seconds": dur}, f"Erreur delta dispatch: {e}")
        logger.error(f"Dispatch différentiel a échoué pour {schema}: {e}")

def run_dispatching():
    """
    Orchestre le processus de dispatching (CRM -> Locaux) en tenant compte des 
    tables vides (Initial) et gère le MIN(MAX(SYNC_DATE)) pour les tables existantes.
    """
    targets = get_target_schemas()
    initial_schemas = []
    delta_schemas = []
    
    # 1. Identifier l'état de chaque cible et récupérer les dates
    max_dates = []
    
    for config, schema in targets:
        count_q = f"SELECT COUNT(*) FROM {schema}.{TARGET_TABLE_CONSO}"
        
        try:
            count = fetch_scalar(config, count_q)
            if count == 0:
                initial_schemas.append((config, schema))
            else:
                date_q = f"SELECT MAX(SYNC_DATE) FROM {schema}.{TARGET_TABLE_CONSO}"
                max_date = fetch_scalar(config, date_q)
                if max_date:
                    max_dates.append(max_date)
                    delta_schemas.append((config, schema))
        except Exception as e:
            logger.error(f"Impossible de vérifier l'état du schéma {schema}: {e}")
            update_monitoring("dispatching", schema, "FAILURE", {}, f"Erreur accès schéma: {e}")
            
    # 2. Préparer le Delta DataFrame si nécessaire
    delta_df = pd.DataFrame()
    if delta_schemas and max_dates:
        min_of_max_dates = min(max_dates) # La date la plus ancienne parmi les MAX(SYNC_DATE)
        logger.info(f"MIN global des MAX(SYNC_DATE) pour dispatching: {min_of_max_dates}")
        
        crm_schema = DB_CONFIG_CRM["central_schema"]
        # Extraire toutes les modifs/ajouts survenus après cette date
        delta_q = f"SELECT * FROM {crm_schema}.{TARGET_TABLE_CONSO} WHERE SYNC_DATE > :1"
        delta_df = execute_select_to_df(DB_CONFIG_CRM, delta_q, (min_of_max_dates,))
        logger.info(f"Taille du Delta extrait: {len(delta_df)} lignes")
        
    # 3. Exécuter l'envoi en parallèle
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        # Envoyer l'initial
        for config, schema in initial_schemas:
            futures.append(executor.submit(dispatch_initial, config, schema))
            
        # Envoyer le delta
        for config, schema in delta_schemas:
            futures.append(executor.submit(dispatch_delta, config, schema, delta_df))
            
        for _ in as_completed(futures):
            pass # Les logs sont déjà gérés dans les sous-fonctions

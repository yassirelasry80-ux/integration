import pandas as pd
import datetime
import time
from .config import DB_CONFIG_CRM, TARGET_TABLE_CONSO
from .db import execute_select_to_df, execute_batch, fetch_scalar
from .logger import update_monitoring, logger

def is_initial_load() -> bool:
    """Vérifie si la table CRM.XIMPAYE_CONSO est complètement vide."""
    schema = DB_CONFIG_CRM["central_schema"]
    count_query = f"SELECT COUNT(*) FROM {schema}.{TARGET_TABLE_CONSO}"
    row_count = fetch_scalar(DB_CONFIG_CRM, count_query)
    return row_count == 0

def get_active_crm_invoices() -> pd.DataFrame:
    """Récupère uniquement les factures actives depuis le CRM."""
    schema = DB_CONFIG_CRM["central_schema"]
    # Facture active: montant réglé < montant global
    query = f"SELECT NUM_0, DOSSIER_0, MNTREG_0, MNTGLB_0 FROM {schema}.{TARGET_TABLE_CONSO} WHERE MNTREG_0 < MNTGLB_0"
    df = execute_select_to_df(DB_CONFIG_CRM, query)
    return df

def process_centralization(source_df: pd.DataFrame):
    """
    Traite la centralisation (Fusion).
    Si CRM est vide -> Initial Load (tout insérer)
    Sinon -> Delta (Inserts nouveaux, Updates paiements partiels, Updates soldes totaux)
    """
    start_time = time.time()
    schema = DB_CONFIG_CRM["central_schema"]
    
    if source_df.empty:
        update_monitoring("centralisation", "CRM_GLOBAL", "SUCCESS", {"duration_seconds": 0}, "Source DataFrame is empty. Noting to centralize.")
        return

    try:
        now_date = datetime.datetime.now()
        source_df["SYNC_DATE"] = now_date

        if is_initial_load():
            logger.info("Détection du Chargement Initial : 0 enregistrement dans le CRM.")
            # Insérer tout massivement
            columns = ", ".join(source_df.columns)
            bind_vars = ", ".join([f":{i+1}" for i in range(len(source_df.columns))])
            insert_query = f"INSERT INTO {schema}.{TARGET_TABLE_CONSO} ({columns}) VALUES ({bind_vars})"
            
            data_to_insert = [tuple(x) for x in source_df.to_numpy()]
            execute_batch(DB_CONFIG_CRM, insert_query, data_to_insert)
            
            duration = round(time.time() - start_time, 2)
            update_monitoring("centralisation", "CRM_GLOBAL", "SUCCESS", {"inserts": len(data_to_insert), "updates": 0, "duration": duration}, "Chargement Initial Complété.")
            return

        # --- Détection Delta ---
        logger.info("Détection du Différentiel pour le CRM.")
        active_crm_df = get_active_crm_invoices()
        
        # Faciliter les comparaisons avec un index composite
        source_df.set_index(["NUM_0", "DOSSIER_0"], inplace=False, drop=False)
        crm_index = active_crm_df.set_index(["NUM_0", "DOSSIER_0"])
        
        insert_data = []
        update_data = []
        
        # Créer un dictionnaire de la source pour accès rapide
        source_dict = source_df.to_dict(orient='index')
        source_keys = set(source_dict.keys())
        crm_keys = set(crm_index.index)
        
        # 1. Nouveaux impayés (INSERT) : Dans la Source mais pas dans le CRM
        new_keys = source_keys - crm_keys
        for k in new_keys:
            insert_data.append(tuple(source_dict[k].values()))
            
        # 2. Paiements partiels (UPDATE) : Dans les deux
        common_keys = source_keys.intersection(crm_keys)
        for k in common_keys:
            crm_mntreg = crm_index.loc[k, 'MNTREG_0']
            src_mntreg = source_dict[k]['MNTREG_0']
            
            # Si le montant réglé a augmenté dans la source
            if src_mntreg > crm_mntreg:
                # Prepare data for UPDATE: MNTREG_0, SYNC_DATE, NUM_0, DOSSIER_0
                update_data.append((src_mntreg, now_date, k[0], k[1]))
                
        # 3. Soldes Totaux (Disparus de la Source mais actifs dans le CRM)
        disappeared_keys = crm_keys - source_keys
        for k in disappeared_keys:
            crm_mntglb = float(crm_index.loc[k, 'MNTGLB_0'])
            # Set MNTREG_0 = MNTGLB_0
            update_data.append((crm_mntglb, now_date, k[0], k[1]))
            
        # Exécution des DML
        if insert_data:
            columns = ", ".join(source_df.columns)
            bind_vars = ", ".join([f":{i+1}" for i in range(len(source_df.columns))])
            insert_query = f"INSERT INTO {schema}.{TARGET_TABLE_CONSO} ({columns}) VALUES ({bind_vars})"
            execute_batch(DB_CONFIG_CRM, insert_query, insert_data)
            
        if update_data:
            update_query = f"UPDATE {schema}.{TARGET_TABLE_CONSO} SET MNTREG_0 = :1, SYNC_DATE = :2 WHERE NUM_0 = :3 AND DOSSIER_0 = :4"
            execute_batch(DB_CONFIG_CRM, update_query, update_data)
            
        duration = round(time.time() - start_time, 2)
        update_monitoring(
            "centralisation", 
            "CRM_GLOBAL", 
            "SUCCESS", 
            {"inserts": len(insert_data), "updates": len(update_data), "duration_seconds": duration}, 
            f"Centralisation OK. Inserts: {len(insert_data)}. Updates: {len(update_data)}."
        )

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        update_monitoring("centralisation", "CRM_GLOBAL", "FAILURE", {"duration_seconds": duration}, f"Erreur de centralisation: {str(e)}")
        logger.error(f"Centralisation erreur: {e}")

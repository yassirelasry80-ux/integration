import time
import os
import signal
import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config import DB_CONFIG_1, DB_CONFIG_2, SYNC_INTERVAL_MINUTES, MAX_RETRIES, RETRY_DELAY_SECONDS
from src.logger import set_global_status, update_monitoring, add_alert, clear_alerts, logger
from src.extraction import extract_schema_data
from src.centralization import process_centralization
from src.dispatch import run_dispatching
from src.integrity import verify_integrity

# Flag to signal a forced run from the Streamlit UI
FORCE_RUN_FILE = "force_sync.flag"

def handle_sigterm(*args):
    logger.info("Signal de terminaison reçu. Arrêt du moteur de synchronisation.")
    set_global_status("STOPPED")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)

def run_extraction_with_retries(db_config, schema):
    """
    Tente l'extraction pour un schéma avec un mécanisme de réessai (retry).
    Renvoie le DataFrame et un booléen indiquant le succès.
    """
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            update_monitoring("extraction", schema, "IN_PROGRESS", {"retries": retries}, f"Tentative {retries + 1}/{MAX_RETRIES + 1}")
            df = extract_schema_data(db_config, schema)
            if not df.empty or df is not None:
                # Assuming extraction.py handles "SUCCESS" logging internally for the metrics
                # We simply add the retries info
                return df, True
            else:
                # If df is empty, it might be an error or just no rows.
                # Assuming extraction.py returns empty df on failure.
                break 
        except Exception as e:
            retries += 1
            msg = f"Erreur d'extraction pour {schema}. Réessai {retries}/{MAX_RETRIES}. Détails: {str(e)}"
            logger.warning(msg)
            update_monitoring("extraction", schema, "FAILURE", {"retries": retries}, msg)
            if retries <= MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
                
    error_msg = f"Échec de l'extraction pour {schema} après {MAX_RETRIES} tentatives."
    logger.error(error_msg)
    add_alert("EXTRACTION_FAIL", error_msg)
    return pd.DataFrame(), False


def orchestrate_sync():
    import datetime
    """Execute un cycle complet de synchronisation."""
    logger.info("Début du cycle de synchronisation.")
    set_global_status("RUNNING", datetime.datetime.now())
    clear_alerts()
    
    # 1. Extraction en Parallèle avec Tolérance aux Pannes
    targets = []
    for schema in DB_CONFIG_1["schemas"]:
        targets.append((DB_CONFIG_1, schema))
    for schema in DB_CONFIG_2["schemas"]:
        targets.append((DB_CONFIG_2, schema))
        
    results = []
    extraction_success_count = 0
    
    with ThreadPoolExecutor(max_workers=min(len(targets), 10)) as executor:
        future_to_schema = {
            executor.submit(run_extraction_with_retries, config, schema): schema 
            for config, schema in targets
        }
        
        for future in as_completed(future_to_schema):
            schema = future_to_schema[future]
            df, success = future.result()
            if success and not df.empty:
                results.append(df)
                extraction_success_count += 1
                
    if extraction_success_count == 0:
        msg = "Toutes les extractions ont échoué. Annulation du cycle."
        logger.error(msg)
        add_alert("CRITICAL_FAIL", msg)
        set_global_status("ERROR")
        return
        
    if extraction_success_count < len(targets):
        add_alert("WARNING", f"Seulement {extraction_success_count}/{len(targets)} sources ont été extraites avec succès.")
    
    global_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    
    # 2. Centralisation (Fusion vers le CRM)
    # The retry logic inside centralisation could be added, but here applying it at orchestration level
    retries = 0
    centralization_ok = False
    while retries <= MAX_RETRIES:
        try:
            update_monitoring("centralisation", "CRM_GLOBAL", "IN_PROGRESS", {"retries": retries}, f"Tentative {retries + 1}")
            process_centralization(global_df)
            centralization_ok = True
            break
        except Exception as e:
            retries += 1
            msg = f"Erreur centralisation. Réessai {retries}/{MAX_RETRIES}. Détails: {e}"
            logger.warning(msg)
            update_monitoring("centralisation", "CRM_GLOBAL", "FAILURE", {"retries": retries}, msg)
            if retries <= MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
                
    if not centralization_ok:
        msg = f"Échec de la centralisation CRM après {MAX_RETRIES} tentatives. Annulation du dispatching."
        logger.error(msg)
        add_alert("CENTRALISATION_FAIL", msg)
        set_global_status("ERROR")
        return
        
    # Vérification Intégrité
    logger.info("Lancement de la vérification d'intégrité...")
    verify_integrity(global_df)
    
    # Verify logic to run dispatch correctly within retries
    # It was previously missing the correct while layout
    dispatch_ok = False
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            update_monitoring("dispatching", "GLOBAL", "IN_PROGRESS", {"retries": retries}, f"Tentative {retries + 1}")
            run_dispatching()
            dispatch_ok = True
            break
        except Exception as e:
            retries += 1
            msg = f"Erreur lors du dispatching global. Réessai {retries}/{MAX_RETRIES}. Détails: {e}"
            logger.warning(msg)
            update_monitoring("dispatching", "GLOBAL", "FAILURE", {"retries": retries}, msg)
            if retries <= MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
                
    if not dispatch_ok:
        msg = f"Échec critique du dispatching après {MAX_RETRIES} tentatives."
        logger.error(msg)
        add_alert("DISPATCH_FAIL", msg)
        set_global_status("ERROR")
        return
        
    logger.info("Cycle de synchronisation terminé avec succès.")
    set_global_status("IDLE")

def start_engine():
    """Démarre le moteur en boucle continue."""
    logger.info(f"Démarrage du Sync Engine. Intervalle défini : {SYNC_INTERVAL_MINUTES} minutes.")
    set_global_status("IDLE")
    
    while True:
        cycle_start = time.time()
        
        try:
            orchestrate_sync()
        except Exception as e:
            logger.error(f"Erreur inattendue dans la boucle principale : {e}")
            add_alert("CRITICAL", f"Erreur inattendue : {e}")
            set_global_status("ERROR")
            
        # Remove the force file if it exists after completing a cycle
        if os.path.exists(FORCE_RUN_FILE):
            os.remove(FORCE_RUN_FILE)
            
        # Wait for the next cycle
        logger.info(f"En attente de {SYNC_INTERVAL_MINUTES} minutes pour le prochain cycle...")
        sleep_time = SYNC_INTERVAL_MINUTES * 60
        
        # Check every second if the force file is created
        for _ in range(sleep_time):
            if os.path.exists(FORCE_RUN_FILE):
                logger.info("Exécution forcée détectée depuis le tableau de bord.")
                break
            time.sleep(1)

if __name__ == "__main__":
    start_engine()

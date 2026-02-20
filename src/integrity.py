import pandas as pd
from .config import DB_CONFIG_CRM, TARGET_TABLE_CONSO
from .db import fetch_scalar
from .logger import update_monitoring, logger

def verify_integrity(source_df: pd.DataFrame):
    """
    Vérifie que le nombre de lignes dans le CRM pour chaque DOSSIER_0
    correspond au nombre de lignes extraites en mémoire.
    """
    if source_df.empty:
        logger.info("Rien à vérifier, extraction vide.")
        return True

    schema = DB_CONFIG_CRM["central_schema"]
    success = True
    
    # Compter par dossier
    source_counts = source_df["DOSSIER_0"].value_counts().to_dict()
    
    for dossier, expected_count in source_counts.items():
        query = f"SELECT COUNT(*) FROM {schema}.{TARGET_TABLE_CONSO} WHERE DOSSIER_0 = :1"
        actual_count = fetch_scalar(DB_CONFIG_CRM, query, (dossier,))
        
        if actual_count == expected_count:
            msg = f"Intégrité OK pour {dossier}: {expected_count} lignes."
            logger.info(msg)
            # Log success to monitoring so the Dashboard can display it
            update_monitoring("centralisation", f"INTEGRITY_{dossier}", "SUCCESS", {"expected": expected_count, "actual": actual_count}, msg)
        else:
            msg = f"MISMATCH Intégrité pour {dossier} ! Attendu: {expected_count} vs Localisé: {actual_count}"
            logger.error(msg)
            
            # Send Email alert (mocked here or use smtplib)
            send_alert_email(subject="ALERTE INTEGRITE SYNCHRO IMPAYES", body=msg)
            
            # Mettre à jour le monitoring pour Streamlit afin de le montrer en FAILURE
            update_monitoring("centralisation", f"INTEGRITY_{dossier}", "FAILURE", {"expected": expected_count, "actual": actual_count}, msg)
            success = False

    return success

def send_alert_email(subject: str, body: str):
    """
    Stub pour envoyer un email d'alerte aux développeurs.
    """
    logger.critical(f"EMAIL ENVOYE:\nSujet: {subject}\nCorps: {body}")
    # TODO: Implementer smtplib.SMTP

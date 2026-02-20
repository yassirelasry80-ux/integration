import json
import os
import datetime
import logging
from .config import MONITORING_FILE

# Configuration de base du logger Python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SyncEngine")

def init_monitoring():
    """Initialise le fichier de monitoring s'il n'existe pas."""
    if not os.path.exists(MONITORING_FILE):
        data = {
            "last_run": None,
            "status": "IDLE",
            "extraction": {},
            "centralisation": {},
            "dispatching": {},
            "alerts": []
        }
        with open(MONITORING_FILE, 'w') as f:
            json.dump(data, f, indent=4)

def update_monitoring(stage, step_name, status, metrics=None, message=""):
    """
    Met à jour une étape spécifique dans le fichier JSON pour le tableau de bord Streamlit.
    stage: 'extraction', 'centralisation', ou 'dispatching'
    step_name: nom de la cible (ex: 'CAS', 'CRM_GLOBAL', etc.)
    status: 'SUCCESS', 'FAILURE', 'IN_PROGRESS'
    metrics: dictionnaire optionnel avec les métriques
    """
    if not os.path.exists(MONITORING_FILE):
        init_monitoring()
        
    try:
        with open(MONITORING_FILE, 'r') as f:
            data = json.load(f)
    except Exception:
        data = {"extraction": {}, "centralisation": {}, "dispatching": {}, "alerts": []}

    if "alerts" not in data:
        data["alerts"] = []

    if stage not in data:
        data[stage] = {}
        
    data[stage][step_name] = {
        "status": status,
        "message": message,
        "timestamp": datetime.datetime.now().isoformat(),
        "metrics": metrics or {}
    }
    
    # Handle specific persistent metrics if provided (like retries)
    if metrics:
        if "retries" in metrics:
            data[stage][step_name]["retries"] = metrics["retries"]
            
    # Écriture atomique (ou quasi-atomique via temp file si nécessaire) pour éviter les corruptions de Streamlit
    with open(MONITORING_FILE, 'w') as f:
        json.dump(data, f, indent=4)
        
    # Log aussi dans la console
    log_msg = f"[{stage.upper()}] {step_name} - {status}"
    if message:
        log_msg += f" : {message}"
    if status == "FAILURE":
        logger.error(log_msg)
    else:
        logger.info(log_msg)

def set_global_status(status, last_run=None):
    if not os.path.exists(MONITORING_FILE):
        init_monitoring()
    
    try:
        with open(MONITORING_FILE, 'r') as f:
            data = json.load(f)
    except Exception:
        data = {}
        
    data["status"] = status
    if last_run:
        data["last_run"] = last_run.isoformat()
        
    with open(MONITORING_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def add_alert(alert_type: str, message: str):
    """
    Ajoute une alerte globale au système.
    alert_type: ex 'EXTRACTION_FAIL', 'DISPATCH_FAIL', 'SYSTEM_ERROR'
    """
    if not os.path.exists(MONITORING_FILE):
        init_monitoring()
        
    try:
        with open(MONITORING_FILE, 'r') as f:
            data = json.load(f)
    except Exception:
        data = {"alerts": []}
        
    if "alerts" not in data:
        data["alerts"] = []
        
    alert = {
        "timestamp": datetime.datetime.now().isoformat(),
        "type": alert_type,
        "message": message
    }
    
    # Ne garder que les 50 dernières alertes pour ne pas surcharger le fichier
    data["alerts"].insert(0, alert)
    data["alerts"] = data["alerts"][:50]
    
    with open(MONITORING_FILE, 'w') as f:
        json.dump(data, f, indent=4)
        
    logger.error(f"[ALERTE] {alert_type}: {message}")

def clear_alerts():
    if not os.path.exists(MONITORING_FILE):
        return
        
    try:
        with open(MONITORING_FILE, 'r') as f:
            data = json.load(f)
            
        data["alerts"] = []
        with open(MONITORING_FILE, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception:
        pass

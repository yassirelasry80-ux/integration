import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime
from config import MONITORING_FILE
import sys

# Ajouter src au path pour les imports locaux depuis la racine du projet
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.sync_engine import FORCE_RUN_FILE

def load_monitoring_data():
    if not os.path.exists(MONITORING_FILE):
        return None
    try:
        with open(MONITORING_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier de monitoring: {e}")
        return None

def main():
    st.set_page_config(page_title="Data Sync Monitor", page_icon="📊", layout="wide")
    st.title("📊 Monitoring de la Synchronisation de Données")

    # Refresh automatic
    st.empty() # placeholder
    
    data = load_monitoring_data()
    
    if not data:
        st.warning("Aucune donnée de monitoring disponible pour le moment.")
        st.info("Le moteur de synchronisation n'a peut-être pas encore démarré.")
        
        if st.button("Démarrer une synchronisation manuelle"):
            with open(FORCE_RUN_FILE, 'w') as f:
                f.write("force")
            st.success("Ordre de démarrage forcé envoyé.")
        return

    # --- Header Metrics ---
    col1, col2, col3 = st.columns(3)
    status = data.get("status", "UNKNOWN")
    
    if status == "RUNNING":
        status_color = "🔵 En cours"
    elif status == "IDLE":
        status_color = "🟢 En attente"
    elif status == "ERROR":
        status_color = "🔴 Erreur Globale"
    else:
        status_color = f"⚪ {status}"
        
    col1.metric("Statut Moteur", status_color)
    
    last_run = data.get("last_run")
    if last_run:
        try:
            last_run_dt = datetime.fromisoformat(last_run).strftime("%Y-%m-%d %H:%M:%S")
        except:
            last_run_dt = last_run
        col2.metric("Dernière Exécution", last_run_dt)
    else:
        col2.metric("Dernière Exécution", "Jamais")
        
    # --- Action Buttons ---
    with col3:
        # Force sync button
        st.write("") # spacing
        if st.button("🚀 Lancer la Synchronisation Maintenant", use_container_width=True):
            if status == "RUNNING":
                st.warning("Une synchronisation est déjà en cours.")
            else:
                with open(FORCE_RUN_FILE, 'w') as f:
                    f.write("force")
                st.success("Ordre de synchronisation forcé envoyé au moteur !")

    st.markdown("---")

    # --- Alerts Region ---
    alerts = data.get("alerts", [])
    if alerts:
        st.subheader("⚠️ Alertes Actives")
        for alert in alerts[:5]: # Mostrar las 5 mas recientes
            timestamp = alert.get("timestamp", "")
            try:
                ts = datetime.fromisoformat(timestamp).strftime("%H:%M:%S")
            except:
                ts = timestamp
                
            msg = f"**[{ts}] {alert.get('type')}**: {alert.get('message')}"
            st.error(msg)
            
        st.markdown("---")


    # --- Detail Tabs ---
    tab1, tab2, tab3, tab4 = st.tabs(["Extractions (Sources -> Mémoire)", "Centralisation (Mémoire -> CRM)", "Dispatching (CRM -> Cibles)", "Vérification Intégrité"])

    # 1. Extractions
    with tab1:
        ext_data = data.get("extraction", {})
        if ext_data:
            df_ext = []
            for schema, info in ext_data.items():
                metrics = info.get("metrics", {})
                df_ext.append({
                    "Schéma Source": schema,
                    "Statut": info.get("status"),
                    "Lignes Extraites": metrics.get("rows_extracted", 0),
                    "Durée (s)": metrics.get("duration_seconds", 0),
                    "Réessais": metrics.get("retries", 0),
                    "Dernier Message": info.get("message", "-")
                })
            st.dataframe(pd.DataFrame(df_ext), use_container_width=True)
        else:
            st.info("Aucune donnée d'extraction.")

    # 2. Centralisation
    with tab2:
        cent_data = data.get("centralisation", {})
        if cent_data:
            df_cent = []
            for target, info in cent_data.items():
                if target.startswith("INTEGRITY_"):
                    continue
                metrics = info.get("metrics", {})
                df_cent.append({
                    "Cible CRM": target,
                    "Statut": info.get("status"),
                    "Inserts (Nouveaux)": metrics.get("inserts", 0),
                    "Updates (Modifiés)": metrics.get("updates", 0),
                    "Durée (s)": metrics.get("duration_seconds", 0),
                    "Réessais": metrics.get("retries", 0),
                    "Dernier Message": info.get("message", "-")
                })
            if df_cent:
                st.dataframe(pd.DataFrame(df_cent), use_container_width=True)
            else:
                st.info("Aucune donnée de centralisation proprement dite.")
        else:
            st.info("Aucune donnée de centralisation.")

    # 3. Dispatching
    with tab3:
        disp_data = data.get("dispatching", {})
        if disp_data:
            df_disp = []
            for schema, info in disp_data.items():
                metrics = info.get("metrics", {})
                df_disp.append({
                    "Cible Locale": schema,
                    "Statut": info.get("status"),
                    "Inserts (Nouveaux)": metrics.get("inserts", 0),
                    "Updates (Modifiés)": metrics.get("updates", 0),
                    "Durée (s)": metrics.get("duration_seconds", 0),
                    "Réessais": metrics.get("retries", 0),
                    "Dernier Message": info.get("message", "-")
                })
            st.dataframe(pd.DataFrame(df_disp), use_container_width=True)
        else:
            st.info("Aucune donnée de dispatching.")
            
    # 4. Intégrité
    with tab4:
        cent_data = data.get("centralisation", {})
        df_integ = []
        for target, info in cent_data.items():
            if target.startswith("INTEGRITY_"):
                dossier = target.replace("INTEGRITY_", "")
                metrics = info.get("metrics", {})
                df_integ.append({
                    "Dossier / Schéma": dossier,
                    "Statut": info.get("status"),
                    "Lignes Attendues": metrics.get("expected", "-"),
                    "Lignes Réelles dans CRM": metrics.get("actual", "-"),
                    "Dernier Constat": info.get("message", "-")
                })
        if df_integ:
            st.dataframe(pd.DataFrame(df_integ), use_container_width=True)
        else:
            st.info("Aucune donnée de vérification d'intégrité enregistrée pour le moment (Si tout est vide, exécutez une extraction d'abord).")

if __name__ == "__main__":
    main()

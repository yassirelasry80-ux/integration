#!/bin/bash
# start.sh - Script pour démarrer l'application Streamlit et (optionnellement) le script de synchro en arrière-plan

echo "Démarrage du Sync Engine en arrière-plan..."
python src/sync_engine.py &

echo "Démarrage de l'application Streamlit..."
streamlit run src/dashboard.py --server.port=8501 --server.address=0.0.0.0

# Optionnel: Si app streamlit s'arrête, garder le conteneur actif
tail -f /dev/null

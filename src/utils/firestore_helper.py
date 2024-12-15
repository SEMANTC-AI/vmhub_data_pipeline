# src/utils/firestore_helper.py

import structlog
from google.cloud import firestore

logger = structlog.get_logger()

def get_customer_data(user_id: str):
    """
    Fetch vmhubToken and cnpj from Firestore path:
    /users/{user_id}/config/settings
    """
    db = firestore.Client()
    settings_ref = db.collection('users').document(user_id).collection('config').document('settings')
    settings_doc = settings_ref.get()

    if not settings_doc.exists:
        raise ValueError(f"No Firestore document found at /users/{user_id}/config/settings")

    data = settings_doc.to_dict()
    vmhub_token = data.get('vmhubToken')
    cnpj = data.get('cnpj')

    if not vmhub_token or not cnpj:
        raise ValueError("Firestore document at /config/settings is missing 'vmhubToken' or 'cnpj'")

    logger.info("Fetched customer data from Firestore", user_id=user_id, cnpj=cnpj)
    return vmhub_token, cnpj
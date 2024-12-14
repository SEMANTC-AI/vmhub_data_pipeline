# src/utils/firestore_helper.py

import structlog
from google.cloud import firestore

logger = structlog.get_logger()

def get_customer_data(user_id: str):
    """fetch vmhubToken and cnpj from Firestore's 'users' collection given a user_id."""
    db = firestore.Client()
    doc_ref = db.collection('users').document(user_id)
    doc = doc_ref.get()

    if not doc.exists:
        raise ValueError(f"no Firestore document found for user_id={user_id}")

    data = doc.to_dict()

    config = data.get('config', {})
    vmhub_token = config.get('vmhubToken')
    cnpj = config.get('cnpj')

    if not vmhub_token or not cnpj:
        raise ValueError("Firestore document missing 'vmhubToken' or 'cnpj' in config")

    logger.info("fetched customer data from Firestore", user_id=user_id, cnpj=cnpj)
    return vmhub_token, cnpj
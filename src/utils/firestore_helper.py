# src/utils/firestore_helper.py
import structlog
from google.cloud import firestore

logger = structlog.get_logger()

def get_customer_data(customer_id: str):
    """Fetch vmhubToken and cnpj from Firestore's 'users' collection given a customer_id."""
    db = firestore.Client()
    doc_ref = db.collection('users').document(customer_id)
    doc = doc_ref.get()
    if not doc.exists:
        raise ValueError(f"No Firestore document found for customer_id={customer_id}")

    data = doc.to_dict()
    vmhub_token = data.get('vmhubToken')
    cnpj = data.get('cnpj')

    if not vmhub_token or not cnpj:
        raise ValueError("Firestore document missing 'vmhubToken' or 'cnpj'")

    logger.info("Fetched customer data from Firestore", customer_id=customer_id, cnpj=cnpj)
    return vmhub_token, cnpj
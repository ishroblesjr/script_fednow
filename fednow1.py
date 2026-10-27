import os
import uuid
import json
import logging
import sqlite3
import base64
import hmac
import hashlib
import time
import urllib.request
import urllib.error
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv

# Initialize production logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [FEDNOW-LIVE] %(message)s')
load_dotenv()

LIVE_API_KEY = os.getenv("COLUMN_LIVE_API_KEY")
DB_PATH = os.getenv("DATABASE_PATH", "ledger_transfers.db")

# Secret key used to sign outhouse webhooks
WEBHOOK_SIGNING_SECRET = "my_secret_webhook_signature_string"

# HARD OVERRIDE: Switched False to completely deactivate the simulation framework
SIMULATION_MODE = False

if not LIVE_API_KEY or not (LIVE_API_KEY.startswith("live_") or LIVE_API_KEY.startswith("test_")):
    logging.critical("CRITICAL ENGINE FAULT: Missing or invalid COLUMN_LIVE_API_KEY inside environment variables.")
    raise RuntimeError("Execution stopped: Production credentials missing or invalid.")

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transfers (
                idempotency_key TEXT PRIMARY KEY,
                amount_cents INTEGER NOT NULL,
                status TEXT NOT NULL,
                column_transfer_id TEXT,
                error_payload TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

init_db()

app = FastAPI(
    title="Asynchronous Production FedNow Settlement Microservice", 
    version="2.0.0",
    description="Live async clearing pipeline connected directly to Column infrastructure."
)
security_scheme = HTTPBearer()

class BankAccountInfo(BaseModel):
    routing_number: str = Field(..., min_length=9, max_length=9)
    account_number: str = Field(..., min_length=4, max_length=17)

class TransferDestination(BankAccountInfo):
    name: str = Field(..., min_length=2)

class FedNowTransferRequest(BaseModel):
    amount_cents: int = Field(..., gt=0)
    description: str = Field("Live FedNow Settlement", max_length=140)
    source: BankAccountInfo
    destination: TransferDestination

def verify_internal_token(credentials: HTTPAuthorizationCredentials = Depends(security_scheme)):
    if credentials.credentials != "4MeuC75WZhOiNOxIQwP8TiqCrcwO7iUvnWQ3dNiGDY":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    return credentials.credentials

def dispatch_webhook_callback(event_type: str, idempotency_key: str, transfer_id: Optional[str], error_data: Optional[dict]):
    target_webhook_url = "http://docker.internal"

    payload = {
        "id": f"evt_{uuid.uuid4().hex[:16]}",
        "event": event_type,
        "created_at": int(time.time()),
        "data": {
            "idempotency_key": idempotency_key,
            "column_transfer_id": transfer_id,
            "error": error_data
        }
    }

    data_bytes = json.dumps(payload).encode('utf-8')
    signature = hmac.new(WEBHOOK_SIGNING_SECRET.encode('utf-8'), data_bytes, hashlib.sha256).hexdigest()

    req = urllib.request.Request(target_webhook_url, data=data_bytes, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-FedNow-Signature", signature)

    try:
        with urllib.request.urlopen(req, timeout=4.0) as response:
            if response.status in (200, 201, 202):
                logging.info(f"✅ [WEBHOOK SUCCESS] Core system acknowledged payload delivery: {response.status}")
    except Exception as net_err:
        logging.error(f"❌ [WEBHOOK TRANSIT FAULT] Connection tracking failure: {net_err}")

def execute_async_transfer_task(unique_tx_id: str, formatted_payload: dict):
    status_state = "FAILED"
    column_id = None
    error_dump = None

    # LIVE GATEWAY: Pointing directly to Column's master live v1 transfers route [1]
    column_endpoint = "https://column.com"
    data_bytes = json.dumps(formatted_payload).encode('utf-8')
    
    req = urllib.request.Request(column_endpoint, data=data_bytes, method="POST")
    
    auth_str = f":{LIVE_API_KEY}".encode("utf-8")
    b64_auth = base64.b64encode(auth_str).decode("utf-8")
    
    req.add_header("Authorization", f"Basic {b64_auth}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Idempotency-Key", unique_tx_id)
    
    logging.info(f"[LIVE DISPATCH] Submitting transaction token to live banking core: {unique_tx_id}")
    
    try:
        with urllib.request.urlopen(req) as response:
            res_body = response.read().decode('utf-8')
            res_json = json.loads(res_body)
            column_id = res_json.get('id')
            status_state = "SUCCESS"
            logging.info(f"💰 [SETTLEMENT CONFIRMED] Live transaction {column_id} successfully settled for reference key: {unique_tx_id}")
    except urllib.error.HTTPError as http_err:
        err_res = http_err.read().decode('utf-8')
        logging.error(f"❌ [DOWNSTREAM BANK REJECTION] Task ID {unique_tx_id} failed with HTTP code {http_err.code}. Details: {err_res}")
        error_dump = json.dumps({"code": http_err.code, "body": err_res[:300]})
    except Exception as base_err:
        logging.error(f"⚠️ [INFRASTRUCTURE FAULT] Network connection to node dropped: {base_err}")
        error_dump = json.dumps({"error": str(base_err)})

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE transfers SET status = ?, column_transfer_id = ?, error_payload = ? WHERE idempotency_key = ?",
            (status_state, column_id, error_dump, unique_tx_id)
        )
        conn.commit()

    event_name = "transfer.settled" if status_state == "SUCCESS" else "transfer.failed"
    error_payload_dict = json.loads(error_dump) if error_dump else None
    dispatch_webhook_callback(event_name, unique_tx_id, column_id, error_payload_dict)

@app.post("/api/v1/transfers/fednow", status_code=status.HTTP_202_ACCEPTED, dependencies=[Depends(verify_internal_token)])
async def queue_money_transfer(payload: FedNowTransferRequest, background_tasks: BackgroundTasks):
    unique_tx_id = f"tx_{uuid.uuid4().hex}"
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO transfers (idempotency_key, amount_cents, status) VALUES (?, ?, ?)", (unique_tx_id, payload.amount_cents, "PENDING"))
        conn.commit()
    
    # LIVE PRODUCTION SCHEMA LAYOUT FORMAT [1]
    formatted_payload = {
        "amount": payload.amount_cents,
        "currency": "USD",
        "description": payload.description,
        "ach_class": "PPD",
        "source": {
            "routing_number": payload.source.routing_number,
            "account_number": payload.source.account_number,
            "account_type": "CHECKING"
        },
        "destination": {
            "routing_number": payload.destination.routing_number,
            "account_number": payload.destination.account_number,
            "name": payload.destination.name,
            "account_type": "CHECKING"
        }
    }
    background_tasks.add_task(execute_async_transfer_task, unique_tx_id, formatted_payload)
    return {"success": True, "status": "ACCEPTED", "idempotency_key": unique_tx_id}

@app.get("/api/v1/transfers/status/{idempotency_key}", dependencies=[Depends(verify_internal_token)])
async def get_transfer_status(idempotency_key: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transfers WHERE idempotency_key = ?", (idempotency_key,))
        row = cursor.fetchone()
        
    if not row:
        raise HTTPException(status_code=404, detail="The requested transaction lookup tracking token was not found.")
        
    return {
        "idempotency_key": row["idempotency_key"],
        "amount_cents": row["amount_cents"],
        "status": row["status"],
        "column_transfer_id": row["column_transfer_id"],
        "error": json.loads(row["error_payload"]) if row["error_payload"] else None,
        "created_at": row["created_at"]
    }

@app.get("/healthz", status_code=200)
async def health_check():
    return {"status": "healthy"}

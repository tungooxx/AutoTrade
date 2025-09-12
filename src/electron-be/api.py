# api.py
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
import pandas as pd
import os
import multiprocessing as mp
import uvicorn
import json
from OptionChainFarmer import run_optionchain, csv_path_for_today, ny_now
from UpdateContractsFarmer import run_updatecontract, run_update_loop
from OptionContractsFarmer import run_optioncontract

from threading import Thread, Event
import numpy as np
app = FastAPI(title="OptionChain API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

#OPTION CHAIN
@app.post("/optionchain/run")
def run():
    # synchronous run; client waits until done
    mp.set_start_method("spawn", force=True)
    df, path, success, bad = run_optionchain()
    return {
        "saved_to": path,
        "rows": int(df.shape[0]) if not df.empty else 0,
        "succeeded": success,
        "invalid_symbols": bad,
        "as_of": ny_now().isoformat()
    }

#UPDATER
_updater_thread: Thread | None = None
_stop_event: Event | None = None
_status = {"running": False, "last_result": None, "last_run_iso": None}
@app.post("/optionupdater/run")
def run(limit: int = 200):
    # synchronous run; client waits until done
    mp.set_start_method("spawn", force=True)
    df, path = run_updatecontract()
    df = pd.read_csv(path, nrows=limit)
    df = df.replace([np.inf, -np.inf], np.nan) 
    records = json.loads(
        df.to_json(orient="records", date_format="iso", date_unit="s")  # NaN -> null
    )
    return JSONResponse(content=records)

@app.post("/optionupdater/start")
def start_updater():
    global _updater_thread, _stop_event
    if _status["running"]:
        return {"running": True, "message": "already running", **_status}
    _stop_event = Event()
    _status["running"] = True
    _updater_thread = Thread(target=run_update_loop, args=(_stop_event, _status), daemon=True)
    _updater_thread.start()
    logger.info("Updater loop started")
    return {"running": True}
#OPTION CONTRACT
@app.post("/optioncontract/run")
def run():
    # synchronous run; client waits until done
    return run_optioncontract()


#TOOLS AND VIEWS
def _require_csv_path():
    path = csv_path_for_today()
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="CSV not found. Run /optionchain/run first.")
    return path

@app.get("/optionchain/preview.csv")
def preview_csv(page: int = 1, page_size: int = 200):
    path = _require_csv_path()

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        total = sum(1 for _ in f) - 1
    total = max(total, 0)

    page = max(1, int(page))
    page_size = max(1, int(page_size))
    offset = (page - 1) * page_size

    df = pd.read_csv(
        path,
        skiprows=range(1, 1 + offset) if offset > 0 else None,
        nrows=page_size,
    )

    return {
        "page": page,
        "page_size": page_size,
        "total": int(total),
        "rows": df.to_dict(orient="records"),
    }

if __name__ == "__main__":
    uvicorn.run("api:app", host="127.0.0.1", port=6789, reload=True)

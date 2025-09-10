# api.py
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
import pandas as pd
import os
import multiprocessing as mp

from OptionChainFarmer import run_optionchain, csv_path_for_today, ny_now
from UpdateContractsFarmer import run_updatecontract
app = FastAPI(title="OptionChain API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

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

@app.post("/optionupdater/run")
def run():
    # synchronous run; client waits until done
    mp.set_start_method("spawn", force=True)
    df, path = run_updatecontract()
    return {
        "saved_to": path,
        "rows": int(df.shape[0]) if not df.empty else 0,
    }

def _require_csv_path():
    path = csv_path_for_today()
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="CSV not found. Run /optionchain/run first.")
    return path

@app.get("/optionchain/preview.csv")
def preview_csv(limit: int = 200):
    path = _require_csv_path()
    df = pd.read_csv(path, nrows=limit)
    return df.to_dict(orient="records")

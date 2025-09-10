# api.py
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
import pandas as pd
import os
import multiprocessing as mp

from OptionChainFarmer import run_optionchain, csv_path_for_today, ny_now
from UpdateContractsFarmer import run_updatecontract, csv_updater_path_for_today
import numpy as np
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
def run(limit: int = 200):
    # synchronous run; client waits until done
    mp.set_start_method("spawn", force=True)
    df, path = run_updatecontract()
    df = pd.read_csv(path, nrows=limit)
    df = df.replace([np.inf, -np.inf], np.nan) 
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")

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

if __name__ == "__main__":
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)

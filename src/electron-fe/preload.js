const { contextBridge, ipcRenderer } = require('electron');

const BASE = 'http://127.0.0.1:6789';

async function jsonFetch(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, { ...opts });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function textFetch(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, { ...opts });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.text();
}
// expose safe, minimal API to the renderer
contextBridge.exposeInMainWorld('api', {
  
  runOptionChain: () =>
    jsonFetch('/optionchain/run', { method: 'POST' }),

  runUpdater: () =>
      jsonFetch('/optionupdater/run', {method: 'POST'}),
  
  previewCSV: (page = 1, pageSize = 200) =>
    jsonFetch(`/optionchain/preview.csv?page=${page}&page_size=${pageSize}`),
  
  Export: () =>
      jsonFetch(`/optioncontract/run`, {method: 'POST'})
  
});

contextBridge.exposeInMainWorld('folder', {
  openFolder: (targetDir) => ipcRenderer.invoke('folder:open', targetDir),
});
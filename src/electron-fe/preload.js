const { contextBridge, ipcRenderer } = require('electron');

const BASE = 'http://127.0.0.1:8000';

async function jsonFetch(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, { ...opts });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}
// expose safe, minimal API to the renderer
contextBridge.exposeInMainWorld('api', {
  ping: () => 'pong',
  
  runOptionChain: () =>
    jsonFetch('/optionchain/run', { method: 'POST' }),

  runUpdater: () =>
      jsonFetch('/optionupdater/run', {method: 'POST'}),
  
  previewCSV: () =>
    jsonFetch(`/optionchain/preview.csv`),
  
});

contextBridge.exposeInMainWorld('folder', {
  openFolder: (targetDir) => ipcRenderer.invoke('folder:open', targetDir),
});

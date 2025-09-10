const out = document.getElementById('out');
const statusEl = document.getElementById('status');
const tableWrap = document.getElementById('table-wrap');

document.getElementById('btn').onclick = () => {
  out.textContent = window.api.ping();
};

document.getElementById('run').onclick = async () => {
  try {
    tableWrap.innerHTML = `<table></table>`;
    statusEl.textContent = 'Running...';
    console.log('[DEBUG] Run button clicked');

    console.log('[DEBUG] Calling runOptionChain API');
    const res = await window.api.runOptionChain();
    console.log('[DEBUG] API response:', res);

    statusEl.textContent = 'Done\n' + JSON.stringify(res, null, 2);
    console.log('[DEBUG] Status element updated');
  } catch (e) {
    statusEl.textContent = `Error: ${e.message}`;
    console.error('[DEBUG] Exception caught:', e);
  }
};

document.getElementById('updater').onclick = async () => {
  try {
    tableWrap.innerHTML = `<table></table>`;
    statusEl.textContent = 'Running...';
    console.log('[DEBUG] Run button clicked');

    console.log('[DEBUG] Calling runOptionChain API');
    const rows = await window.api.runUpdater();
    statusEl.textContent = `Rows: ${rows.length}`;
    console.log('[DEBUG] response rows:', rows?.length ?? 0);
    const cols = Object.keys(rows[0]);
    const thead = `<thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>`;
    console.log(thead);
    const tbody = `<tbody>${
      rows.map(r => `<tr>${cols.map(c => `<td>${r[c] ?? ''}</td>`).join('')}</tr>`).join('')
    }</tbody>`;
    tableWrap.innerHTML = `<table>${thead}${tbody}</table>`;
  } catch (e) {
    statusEl.textContent = `Error: ${e.message}`;
    console.error('[DEBUG] Exception caught:', e);
  }
};

document.getElementById('preview').onclick = async () => {
  try {
    tableWrap.innerHTML = `<table></table>`;
    statusEl.textContent = 'Loading preview…';
    const rows = await window.api.previewCSV();
    statusEl.textContent = `Rows: ${rows.length}`;
    const cols = Object.keys(rows[0]);
    const thead = `<thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>`;
    console.log(thead);
    const tbody = `<tbody>${
      rows.map(r => `<tr>${cols.map(c => `<td>${r[c] ?? ''}</td>`).join('')}</tr>`).join('')
    }</tbody>`;
    tableWrap.innerHTML = `<table>${thead}${tbody}</table>`;
  }
  catch (e){
    statusEl.textContent = `Error: ${e.message}`;
  }
};

document.getElementById('open').onclick = async () => { 
  await window.folder.openFolder('F:\\Newgeneration\\AI-Coffee\\ib-electron\\option_chain_ATM_tickers'); 
};

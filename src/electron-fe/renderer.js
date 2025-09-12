const statusEl = document.getElementById('status');
const tableWrap = document.getElementById('table-wrap');


document.getElementById('run').onclick = async () => {
  running = false;
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

let running = false;
const intervalEl = document.getElementById('intervalSec');
async function runLoop() {
  while (running) {
    try {
      tableWrap.innerHTML = `<table></table>`;
      statusEl.textContent = 'Running...';
      console.log('[DEBUG] Run button clicked');

      console.log('[DEBUG] Calling runOptionChain API');
      const rows = await window.api.runUpdater();
      const raw = intervalEl.value
      statusEl.textContent = `Rows: ${rows.length}, Wait for another ${raw}s`;
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
    if (running) {
      const sec = Math.max(5, Number(intervalEl.value) || 60);
      await new Promise(r => setTimeout(r, sec * 1000));
    }
  }
}



document.getElementById('updater').onclick = async () => {
  if (running) {
    statusEl.textContent = 'Already running';
    return;
  }
  running = true;
  statusEl.textContent = 'Started updater loop';
  runLoop();
};

document.getElementById('stop').onclick = () => {
  running = false;
  statusEl.textContent = 'Stopped';
};

document.getElementById('export').onclick = async () => {
  running = false;
  statusEl.textContent = 'Farming contract…';
  const data = await window.api.Export();
  statusEl.textContent = "Done, all csv saved in Option Ticker folder";
};

document.getElementById('preview').onclick = async () => {
  running = false;
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
  await window.folder.openFolder(''); 
};

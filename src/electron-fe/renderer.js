const statusEl = document.getElementById('status');
const tableWrap = document.getElementById('table-wrap');
const pageInfo  = document.getElementById('pageInfo');
const btnPrev   = document.getElementById('prevPage');
const btnNext   = document.getElementById('nextPage');
const intervalEl = document.getElementById('intervalSec');
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
async function runLoop() {
  while (running) {
    try {
      tableWrap.innerHTML = `<table></table>`;
      statusEl.textContent = 'Running...';
      console.log('[DEBUG] Run button clicked');

      console.log('[DEBUG] Calling runOptionChain API');
      const rows = await window.api.runUpdater();
      const raw = intervalEl.value;
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


document.getElementById('open').onclick = async () => {
  await window.folder.openFolder(''); 
};

let curPage = 1;
const PAGE_SIZE = 200;
let totalRows = 0;

async function loadPage(p) {
  statusEl.textContent = 'Loading preview…';
  const res = await window.api.previewCSV(p, PAGE_SIZE);
  const rows = res.rows || [];
  totalRows = res.total ?? rows.length;
  curPage = res.page ?? p;

  statusEl.textContent = `Rows: ${totalRows}`;
  pageInfo.textContent = `Page ${curPage} · ${PAGE_SIZE}/page · Total ${totalRows}`;

  if (!rows.length) {
    tableWrap.innerHTML = '<div class="muted">No data</div>';
    btnPrev.disabled = curPage <= 1;
    btnNext.disabled = curPage * PAGE_SIZE >= totalRows;
    return;
  }

  const cols = Object.keys(rows[0] || {});
  const thead = `<thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>`;
  const tbody = `<tbody>${
    rows.map(r => `<tr>${cols.map(c => `<td>${r[c] ?? ''}</td>`).join('')}</tr>`).join('')
  }</tbody>`;
  tableWrap.innerHTML = `<table>${thead}${tbody}</table>`;

  btnPrev.disabled = curPage <= 1;
  btnNext.disabled = curPage * PAGE_SIZE >= totalRows;
}

document.getElementById('preview').onclick = () => loadPage(1);
btnPrev.onclick = () => { if (curPage > 1) loadPage(curPage - 1); };
btnNext.onclick = () => { if (curPage * PAGE_SIZE < totalRows) loadPage(curPage + 1); };
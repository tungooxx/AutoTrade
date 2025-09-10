const { app, BrowserWindow, ipcMain, shell } = require('electron');
const path = require('path');

function createWindow() {
    const base = __dirname;
    const win = new BrowserWindow({
        width: 1280, height:1024,
        webPreferences: {
            preload: path.join(base, 'preload.js'),
            contextIsolation: true,
            nodeIntegration: false,
            sandbox: true
        }
    });
    win.loadFile(path.join(base, 'index.html'));
}

ipcMain.handle('folder:open', async (_evt, targetDir) => {
  if (!targetDir) return { ok: false, error: 'No directory provided' };
  await shell.openPath(targetDir);
  return { ok: true };
});

app.on('certificate-error', (event, webContents, url, error, certificate, callback) => {
  if (url.startsWith('https://localhost:')) { event.preventDefault(); callback(true); }
  else callback(false);
});

app.whenReady().then(createWindow);

app.on('window-all-closed', () => { if (process.platform !== 'darwin') app.quit(); });
app.on('activate', () => { if (BrowserWindow.getAllWindows().length === 0) createWindow(); });
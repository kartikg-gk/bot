const { app, BrowserWindow, clipboard, dialog, ipcMain, safeStorage, shell } = require("electron");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const { CREDENTIALS, Gateway, Preferences } = require("./gateway.cjs");

const html = path.join(__dirname, "index.html");
const renderer = pathToFileURL(html).href;
let window;
let queue = Promise.resolve();
let initialized = false;

function openWindow() {
  if (window && !window.isDestroyed()) {
    window.focus();
    return;
  }
  window = new BrowserWindow({
    title: "Delta Gateway", width: 900, height: 740, minWidth: 620, minHeight: 540,
    webPreferences: {
      preload: path.join(__dirname, "preload.cjs"), sandbox: true,
      contextIsolation: true, nodeIntegration: false,
    },
  });
  window.webContents.setWindowOpenHandler(() => ({ action: "deny" }));
  window.webContents.on("will-navigate", event => event.preventDefault());
  window.webContents.session.setPermissionRequestHandler((_contents, _permission, callback) => callback(false));
  window.on("closed", () => { window = null; });
  return window.loadFile(html);
}

function handle(name, action) {
  ipcMain.handle("gateway:" + name, (event, ...args) => {
    if (!window || event.sender !== window.webContents ||
        event.senderFrame !== window.webContents.mainFrame || event.senderFrame.url !== renderer) {
      throw new Error("Unexpected desktop request.");
    }
    const next = queue.then(() => action(...args));
    queue = next.catch(() => {});
    return next;
  });
}

async function choose(options) {
  const result = await dialog.showOpenDialog(window, options);
  return result.canceled ? null : result.filePaths[0];
}

async function startup() {
  const state = app.getPath("userData");
  const settings = new Preferences(path.join(state, "settings.json"), safeStorage);
  await settings.read();
  const gateway = new Gateway(settings, state);
  handle("settings", () => ({
    binary: settings.data.binary, selected: settings.data.selected, credentials: CREDENTIALS,
  }));
  handle("chooseBinary", async () => {
    const binary = await choose({ title: "Choose the delta-gateway executable", properties: ["openFile"] });
    if (!binary) return null;
    const version = await gateway.run(binary, ["--version"]);
    if (!/^delta-gateway \S+/.test(version)) throw new Error("That executable is not the Delta Gateway CLI.");
    await settings.update({ binary });
    return binary;
  });
  handle("list", () => gateway.list());
  handle("status", config => gateway.status(config));
  handle("select", config => gateway.select(config));
  handle("import", async () => {
    const config = await choose({
      title: "Choose a gateway configuration", properties: ["openFile"],
      filters: [{ name: "Gateway configuration", extensions: ["yaml", "yml", "json"] }],
    });
    return config ? gateway.import(config) : null;
  });
  handle("chooseProject", () => choose({ title: "Choose a project folder", properties: ["openDirectory"] }));
  handle("setup", (project, name, executor, image, provider, model) =>
    gateway.setup(project, name, executor, image, provider, model));
  handle("start", config => gateway.change(config, "start"));
  handle("stop", config => gateway.change(config, "stop"));
  handle("logs", config => gateway.logs(config));
  handle("saveCredential", (config, name, value) => gateway.save(config, name, value));
  handle("copyWebToken", async config => clipboard.writeText(await gateway.webToken(config)));
  handle("openDashboard", async config => {
    const { dashboard } = await gateway.status(config);
    if (!dashboard) throw new Error("A dashboard URL is available when a web gateway is ready.");
    await shell.openExternal(dashboard);
  });
  initialized = true;
  await openWindow();
}

if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  app.on("second-instance", () => { if (initialized) openWindow(); });
  app.whenReady().then(startup).catch(error => {
    dialog.showErrorBox("Delta Gateway", error.message);
    app.quit();
  });
  app.on("activate", () => { if (initialized) openWindow(); });
  app.on("window-all-closed", () => { if (process.platform !== "darwin") app.quit(); });
}

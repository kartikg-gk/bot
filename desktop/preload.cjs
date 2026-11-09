const { contextBridge, ipcRenderer } = require("electron");

const request = (name, ...args) => ipcRenderer.invoke("gateway:" + name, ...args);
contextBridge.exposeInMainWorld("gateway", Object.freeze({
  settings: () => request("settings"),
  chooseBinary: () => request("chooseBinary"),
  list: () => request("list"),
  status: config => request("status", config),
  select: config => request("select", config),
  importConfig: () => request("import"),
  chooseProject: () => request("chooseProject"),
  setup: (project, name, executor, image, provider, model) =>
    request("setup", project, name, executor, image, provider, model),
  start: config => request("start", config),
  stop: config => request("stop", config),
  logs: config => request("logs", config),
  saveCredential: (config, name, value) => request("saveCredential", config, name, value),
  copyWebToken: config => request("copyWebToken", config),
  openDashboard: config => request("openDashboard", config),
}));

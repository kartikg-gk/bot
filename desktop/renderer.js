const byId = id => document.getElementById(id);
const api = window.gateway;
const picker = byId("instances");
let current = null;
let project = null;
let busy = false;
let polling = false;

function show(status = current) {
  current = status;
  byId("status").textContent = status
    ? (status.name || status.instance.slice(0, 8)) + " · " + status.state + (status.pid ? " · PID " + status.pid : "") +
      " · Scheduler " + status.scheduler.state
    : "Select a gateway configuration.";
  byId("saved").textContent = status?.saved.length ? "Saved here: " + status.saved.join(", ") : "";
  byId("start").disabled = busy || !status || status.running;
  byId("stop").disabled = busy || !status?.running;
  byId("dashboard").disabled = busy || !status?.dashboard;
  byId("copy-token").disabled = busy || status?.web_auth !== "bearer";
  byId("logs").disabled = busy || !status;
  byId("save-credential").disabled = busy || !status || status.running;
  byId("create").disabled = busy || !project;
  for (const id of ["instances", "import", "refresh", "choose-project", "binary"]) {
    byId(id).disabled = busy;
  }
}

async function action(callback) {
  if (busy) return;
  busy = true;
  byId("error").textContent = "";
  byId("notice").textContent = "";
  show();
  try { await callback(); }
  catch (error) { byId("error").textContent = error.message.replace(/^Error invoking remote method .*?: Error: /, ""); }
  finally { busy = false; show(); }
}

async function list(preferred = current?.config) {
  const rows = await api.list();
  const configs = rows.map(row => new Option(row.name, row.config));
  if (preferred && !rows.some(row => row.config === preferred)) {
    configs.push(new Option(current?.name || preferred, preferred));
  }
  picker.replaceChildren(...configs);
  if (preferred) picker.value = preferred;
  if (picker.value) show(await api.select(picker.value));
  else show(null);
}

byId("binary").onclick = () => action(async () => {
  const binary = await api.chooseBinary();
  if (binary) {
    byId("binary-path").textContent = binary;
    await list();
  }
});
byId("import").onclick = () => action(async () => {
  const status = await api.importConfig();
  if (status) { show(status); await list(status.config); }
});
byId("refresh").onclick = () => action(() => list());
picker.onchange = () => action(async () => {
  try {
    show(await api.select(picker.value));
    byId("log-output").textContent = "";
  } catch (error) {
    if (current) picker.value = current.config;
    throw error;
  }
});
byId("choose-project").onclick = () => action(async () => {
  const choice = await api.chooseProject();
  if (choice) {
    project = choice;
    byId("project").textContent = choice;
    byId("name").value = choice.split(/[\\/]/).filter(Boolean).pop()
      ?.replace(/[^a-zA-Z0-9-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 64) || "gateway";
  }
});
byId("setup").onsubmit = event => {
  event.preventDefault();
  action(async () => {
    const status = await api.setup(project, byId("name").value, byId("executor").value,
      byId("image").value.trim(), byId("provider").value, byId("model").value.trim());
    show(status);
    await list(status.config);
    byId("notice").textContent = "Gateway created. Start it, then open the dashboard.";
  });
};
byId("executor").onchange = () => {
  const executor = byId("executor").value;
  byId("image").disabled = !executor || executor === "local";
  byId("image").required = executor === "docker";
  if (byId("image").disabled) byId("image").value = "";
};

for (const name of ["start", "stop"]) {
  byId(name).onclick = () => action(async () => {
    byId("notice").textContent = name === "start" ? "Starting gateway…" : "Stopping gateway…";
    try { show(await api[name](current.config)); }
    finally { byId("notice").textContent = ""; }
  });
}
byId("dashboard").onclick = () => action(() => api.openDashboard(current.config));
byId("copy-token").onclick = () => action(async () => {
  await api.copyWebToken(current.config);
  byId("notice").textContent = "Web token copied.";
});
byId("logs").onclick = () => action(async () => {
  byId("log-output").textContent = await api.logs(current.config);
});
byId("credential-form").onsubmit = event => {
  event.preventDefault();
  action(async () => {
    const field = byId("credential");
    const secret = field.value;
    field.value = "";
    show(await api.saveCredential(current.config, byId("credential-name").value, secret));
    byId("notice").textContent = secret ? "Credential saved." : "Saved credential removed.";
  });
};

setInterval(async () => {
  if (busy || polling || !current) return;
  const config = current.config;
  polling = true;
  try {
    const status = await api.status(config);
    if (!busy && current?.config === config) show(status);
  } catch (error) {
    if (!busy && current?.config === config) byId("error").textContent = error.message;
  } finally { polling = false; }
}, 3000);

action(async () => {
  const settings = await api.settings();
  byId("binary-path").textContent = settings.binary;
  byId("credential-name").replaceChildren(...settings.credentials.map(name => new Option(name)));
  await list(settings.selected);
});

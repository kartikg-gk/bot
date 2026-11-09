const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const { tmpdir } = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const { Gateway, Preferences, localDashboard, runCli } = require("../gateway.cjs");

const instance = "a".repeat(32);
const WEB_TOKEN = "DELTA_GATEWAY_WEB_TOKEN";

async function fixture(t) {
  const directory = await fs.mkdtemp(path.join(tmpdir(), "delta-desktop-"));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const crypto = {
    isAsyncEncryptionAvailable: async () => true,
    encryptStringAsync: async text => Buffer.from("sealed:" + text),
    decryptStringAsync: async bytes => ({ result: bytes.toString().slice(7) }),
  };
  const preferences = new Preferences(path.join(directory, "settings.json"), crypto);
  await preferences.read();
  const commands = [];
  let running = false;
  let config = null;
  const gateway = new Gateway(preferences, directory, async (binary, args, env) => {
    assert.equal(binary, "delta-gateway");
    commands.push({ args, env });
    if (args[0] === "init") config = args[2];
    if (args[0] === "start") running = true;
    if (args[0] === "stop") running = false;
    if (args[0] === "instances") return JSON.stringify(config ? [{ name: "Gateway", config }] : []);
    if (args[0] === "logs") return "token=" + (await preferences.credentials(instance))[WEB_TOKEN];
    if (args[0] === "status") return JSON.stringify({
      instance, name: "Gateway", config: args[2], running,
      state: running ? "ready" : "stopped",
      web_auth: "bearer", dashboard: running ? "http://127.0.0.1:9999" : null,
      scheduler: { state: running ? "online" : "stopped" },
    });
    return "";
  });
  return { directory, preferences, commands, gateway };
}

test("setup and lifecycle reuse the installed CLI and keep secrets out of arguments", async t => {
  const { directory, preferences, commands, gateway } = await fixture(t);
  await assert.rejects(gateway.status(path.join(directory, "unknown.yml")), /registered/);
  await assert.rejects(gateway.setup(directory, "New", "docker", "", "", ""), /Docker image/);
  const before = await gateway.setup(directory, "New", "docker", "python:3.12-slim", "openai", "model");
  const init = commands.find(command => command.args[0] === "init");
  assert.deepEqual(init.args.slice(3), ["--project", directory, "--name", "New",
    "--web-only", "--web-port", "0", "--executor", "docker", "--docker-image", "python:3.12-slim",
    "--provider", "openai", "--model", "model"]);
  assert.deepEqual(before.saved, [WEB_TOKEN]);
  const token = await gateway.webToken(before.config);
  assert.ok(token.length >= 32);
  assert.ok(!(await fs.readFile(preferences.filename, "utf8")).includes(token));
  const running = await gateway.change(before.config, "start");
  assert.equal(running.scheduler.state, "online");
  assert.equal(running.dashboard, "http://127.0.0.1:9999/");
  const launch = commands.find(command => command.args[0] === "start");
  assert.equal(launch.env[WEB_TOKEN], token);
  assert.ok(!launch.args.includes(token));
  await assert.rejects(gateway.save(before.config, WEB_TOKEN, "new-web-test-value"), /Stop the gateway/);
  assert.equal(await gateway.logs(before.config), "token=[redacted]");
  assert.equal((await gateway.change(before.config, "stop")).state, "stopped");
  assert.equal((await gateway.save(before.config, WEB_TOKEN, "")).saved.length, 0);
});

test("known configurations and credential names are checked", async t => {
  const { directory, gateway } = await fixture(t);
  await assert.rejects(gateway.import(path.join(directory, "wrong.exe")), /YAML or JSON/);
  const config = path.join(directory, "existing.yaml");
  assert.equal((await gateway.import(config)).config, config);
  await assert.rejects(gateway.change(config, "remove"), /Invalid/);
  await assert.rejects(gateway.save(config, "PATH", "/bin"), /Unsupported/);
});

test("dashboard URLs allow HTTPS and loopback HTTP without embedded credentials", () => {
  assert.equal(localDashboard("https://dashboard.example"), "https://dashboard.example/");
  assert.equal(localDashboard("http://[::1]:8123"), "http://[::1]:8123/");
  for (const value of ["javascript:alert(1)", "file:///tmp/file", "http://example.test",
    "https://user:pass@dashboard.example", "http://127.0.0.1/?token=secret"]) {
    assert.equal(localDashboard(value), null);
  }
});

test("CLI adapter passes shell punctuation literally", async () => {
  const value = "a & b; $(echo fail)";
  const output = await runCli(process.execPath, ["-e", "console.log(process.argv[1])", value]);
  assert.equal(output.trim(), value);
});

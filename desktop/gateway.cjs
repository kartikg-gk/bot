const { execFile } = require("node:child_process");
const { randomBytes, randomUUID } = require("node:crypto");
const fs = require("node:fs/promises");
const { homedir } = require("node:os");
const path = require("node:path");
const { promisify } = require("node:util");

const exec = promisify(execFile);
const WEB_TOKEN = "DELTA_GATEWAY_WEB_TOKEN";
const CREDENTIALS = Object.freeze([
  WEB_TOKEN, "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "OPENROUTER_API_KEY",
  "TELEGRAM_BOT_TOKEN", "DAYTONA_API_KEY", "DELTA_GATEWAY_GOOGLE_CLIENT_ID",
  "DELTA_GATEWAY_GOOGLE_CLIENT_SECRET", "DELTA_GATEWAY_MASTER_KEY",
]);

async function runCli(binary, args, environment = {}) {
  const extra = [path.join(homedir(), ".local", "bin"), "/opt/homebrew/bin", "/usr/local/bin"];
  const env = { ...process.env, ...environment };
  env.PATH = [env.PATH, ...extra].filter(Boolean).join(path.delimiter);
  try {
    const { stdout } = await exec(binary, args, { env, timeout: 310_000, maxBuffer: 1024 * 1024 });
    return redact(stdout, environment);
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Delta Gateway CLI is missing. Install it or choose its executable.");
    }
    const message = String(error.stderr || error.stdout || "").trim();
    throw new Error(redact(message || "The gateway command failed or timed out.", environment));
  }
}

function redact(text, environment) {
  const secrets = CREDENTIALS.map(name => environment[name] || process.env[name])
    .filter(value => typeof value === "string" && value.length >= 8)
    .sort((left, right) => right.length - left.length);
  for (const value of secrets) text = text.replaceAll(value, "[redacted]");
  return text;
}

function localDashboard(candidate) {
  if (typeof candidate !== "string") return null;
  let url;
  try { url = new URL(candidate); } catch { return null; }
  if (url.username || url.password || url.search || url.hash) return null;
  if (url.protocol === "https:") return url.href;
  const host = url.hostname;
  const loopback = host === "localhost" || host === "[::1]" || /^127\.(\d+\.){2}\d+$/.test(host);
  return url.protocol === "http:" && loopback ? url.href : null;
}

class Preferences {
  constructor(filename, encryption) {
    this.filename = filename;
    this.encryption = encryption;
    this.data = { version: 1, binary: "delta-gateway", selected: null, credentials: {} };
  }

  async read() {
    try {
      const data = JSON.parse(await fs.readFile(this.filename, "utf8"));
      if (data.version !== 1 || typeof data.credentials !== "object" || !data.credentials) {
        throw new Error("Desktop settings have an unsupported format.");
      }
      this.data = data;
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }

  async update(next) {
    const directory = path.dirname(this.filename);
    await fs.mkdir(directory, { recursive: true, mode: 0o700 });
    const staging = path.join(directory, "settings-" + randomUUID() + ".tmp");
    const data = { ...this.data, ...next };
    try {
      await fs.writeFile(staging, JSON.stringify(data), { mode: 0o600, flag: "wx" });
      await fs.rename(staging, this.filename);
    } finally {
      await fs.rm(staging, { force: true });
    }
    this.data = data;
  }

  async seal(secret) {
    if (!await this.encryption.isAsyncEncryptionAvailable()) {
      throw new Error("Secure storage is unavailable. Unlock the macOS Keychain and try again.");
    }
    return (await this.encryption.encryptStringAsync(secret)).toString("base64");
  }

  async credentials(instance) {
    const result = {};
    const saved = this.data.credentials[instance] || {};
    for (const name of CREDENTIALS) {
      if (!saved[name]) continue;
      const { result: value } = await this.encryption.decryptStringAsync(Buffer.from(saved[name], "base64"));
      if (typeof value !== "string" || !value) throw new Error("The saved credential could not be unlocked.");
      result[name] = value;
    }
    return result;
  }

  async saveCredential(instance, name, secret) {
    if (!CREDENTIALS.includes(name)) throw new Error("Unsupported credential.");
    if (typeof secret !== "string" || !/^[!-~]{0,4096}$/.test(secret)) {
      throw new Error("Enter a credential of up to 4096 printable characters.");
    }
    if (name === WEB_TOKEN && secret && secret.length < 16) {
      throw new Error("A web token must contain at least 16 characters.");
    }
    const saved = { ...this.data.credentials[instance] };
    if (secret) saved[name] = await this.seal(secret);
    else delete saved[name];
    await this.update({ credentials: { ...this.data.credentials, [instance]: saved } });
  }
}

class Gateway {
  constructor(preferences, state, run = runCli) {
    this.preferences = preferences;
    this.state = state;
    this.run = run;
    this.known = new Set();
    if (preferences.data.selected) this.known.add(preferences.data.selected);
  }

  async call(args, environment) {
    return this.run(this.preferences.data.binary, args, environment);
  }

  async list() {
    const entries = JSON.parse(await this.call(["instances", "list", "--json"]));
    if (!Array.isArray(entries)) throw new Error("The CLI returned an invalid instance list.");
    for (const entry of entries) {
      if (typeof entry.config === "string" && path.isAbsolute(entry.config)) this.known.add(entry.config);
    }
    return entries;
  }

  check(config) {
    if (!this.known.has(config)) throw new Error("Choose a registered gateway configuration first.");
  }

  async select(config) {
    this.check(config);
    const status = await this.status(config);
    if (this.preferences.data.selected !== config) await this.preferences.update({ selected: config });
    return status;
  }

  async import(config) {
    if (!path.isAbsolute(config) || !/\.(yaml|yml|json)$/i.test(config)) {
      throw new Error("Choose a YAML or JSON gateway configuration.");
    }
    this.known.add(config);
    return this.select(config);
  }

  async status(config) {
    this.check(config);
    const result = JSON.parse(await this.call(["status", "--config", config, "--json"]));
    if (!/^[a-f0-9]{32}$/.test(result.instance) || typeof result.running !== "boolean") {
      throw new Error("The CLI returned invalid gateway status.");
    }
    result.dashboard = localDashboard(result.dashboard);
    result.saved = CREDENTIALS.filter(name => this.preferences.data.credentials[result.instance]?.[name]);
    return result;
  }

  async setup(project, name, executor, image, provider, model) {
    if (!path.isAbsolute(project) || !/^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$/.test(name)) {
      throw new Error("Choose a project folder and a name of up to 64 letters, numbers, and dashes.");
    }
    if (!(await fs.stat(project)).isDirectory()) throw new Error("Choose a project folder.");
    if (!["local", "docker", "daytona"].includes(executor)) throw new Error("Choose an executor.");
    if (typeof image !== "string" || image && !/^[a-zA-Z0-9][a-zA-Z0-9._:@/-]{0,199}$/.test(image)) {
      throw new Error("Enter a valid image or snapshot name.");
    }
    if (executor === "docker" && !image) throw new Error("Choose a Docker image.");
    if (!["", "openai", "anthropic", "openrouter"].includes(provider)) throw new Error("Choose a provider.");
    if (typeof model !== "string" || model && !/^[a-zA-Z0-9][a-zA-Z0-9._:/@+-]{0,199}$/.test(model)) {
      throw new Error("Enter a valid model ID.");
    }
    const token = randomBytes(32).toString("base64url");
    const sealed = await this.preferences.seal(token);
    const folder = path.join(this.state, "instances");
    await fs.mkdir(folder, { recursive: true, mode: 0o700 });
    const config = path.join(folder, randomUUID() + ".yaml");
    const args = ["init", "--config", config, "--project", project, "--name", name,
      "--web-only", "--web-port", "0", "--executor", executor];
    if (executor === "docker") args.push("--docker-image", image);
    if (executor === "daytona" && image) args.push("--daytona-snapshot", image);
    if (provider) args.push("--provider", provider);
    if (model) args.push("--model", model);
    await this.call(args);
    this.known.add(config);
    const status = await this.status(config);
    await this.preferences.update({
      selected: config,
      credentials: { ...this.preferences.data.credentials, [status.instance]: { [WEB_TOKEN]: sealed } },
    });
    return this.status(config);
  }

  async change(config, action) {
    if (!["start", "stop"].includes(action)) throw new Error("Invalid gateway operation.");
    const status = await this.status(config);
    const environment = action === "start" ? await this.preferences.credentials(status.instance) : {};
    await this.call([action, "--config", config], environment);
    return this.status(config);
  }

  async logs(config) {
    const status = await this.status(config);
    const environment = await this.preferences.credentials(status.instance);
    return redact(await this.call(["logs", "--config", config, "--lines", "150"]), environment);
  }

  async save(config, name, secret) {
    const status = await this.status(config);
    if (status.running) throw new Error("Stop the gateway before changing its saved credentials.");
    await this.preferences.saveCredential(status.instance, name, secret);
    return this.status(config);
  }

  async webToken(config) {
    const status = await this.status(config);
    if (status.web_auth !== "bearer") throw new Error("This gateway does not use a web token.");
    const saved = await this.preferences.credentials(status.instance);
    const token = saved[WEB_TOKEN] || process.env[WEB_TOKEN];
    if (!token) throw new Error("No web token is available to this app.");
    return token;
  }
}

module.exports = { CREDENTIALS, Gateway, Preferences, localDashboard, runCli };

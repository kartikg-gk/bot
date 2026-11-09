import http from "node:http";
import https from "node:https";

const PROXY_HEADER = "x-delta-gateway-proxy";
const HOP_HEADERS = [
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade",
];

function origin(value) {
  const url = new URL(value);
  const loopback = ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname);
  if (url.href !== url.origin + "/" ||
      !(url.protocol === "https:" || (loopback && url.protocol === "http:"))) {
    throw new Error("Expected an HTTPS origin or a loopback HTTP origin");
  }
  return url;
}

function config(env) {
  const upstream = origin(env.DELTA_GATEWAY_UPSTREAM_URL);
  const publicOrigin = origin(env.DELTA_GATEWAY_PUBLIC_ORIGIN);
  const secret = env.DELTA_GATEWAY_PROXY_SECRET;
  if (publicOrigin.origin === upstream.origin ||
      typeof secret !== "string" || !/^[A-Za-z0-9_-]{43,128}$/.test(secret)) {
    throw new Error("Distinct origins and an ingress secret are required");
  }
  return { upstream, publicOrigin, secret };
}

function endToEnd(headers) {
  const skip = new Set(HOP_HEADERS);
  for (const name of (headers.connection ?? "").split(",")) {
    skip.add(name.trim().toLowerCase());
  }
  return Object.fromEntries(Object.entries(headers).filter(([name]) =>
    !skip.has(name) && name !== PROXY_HEADER && name !== "forwarded" &&
    !name.startsWith("x-forwarded-") && !name.startsWith("x-vercel-"),
  ));
}

function error(res, status, message) {
  res.writeHead(status, {
    "content-type": "application/json",
    "cache-control": "private, no-store",
  });
  res.end(JSON.stringify({ error: message }));
}

export function gatewayProxy(env = process.env, lifetimeMs = 50_000) {
  return function proxy(req, res) {
    let settings;
    try {
      settings = config(env);
    } catch {
      return error(res, 503, "Gateway proxy is not configured");
    }
    const { upstream, publicOrigin, secret } = settings;
    if (req.headers.host?.toLowerCase() !== publicOrigin.host) {
      return error(res, 421, "Unexpected gateway host");
    }
    const target = req.originalUrl ?? req.url;
    const path = target.split("?", 1)[0];
    if (!(path === "/" || path.startsWith("/api/") || path.startsWith("/auth/")) ||
        /\\|%2f|%5c|%25/i.test(path) || new URL(target, publicOrigin).pathname !== path) {
      return error(res, 404, "Gateway route not found");
    }

    const headers = endToEnd(req.headers);
    delete headers.host;
    delete headers.expect;
    headers[PROXY_HEADER] = secret;
    const transport = upstream.protocol === "https:" ? https : http;
    const request = transport.request(upstream, {
      method: req.method,
      path: target,
      headers,
    });
    let streaming = false;
    const deadline = setTimeout(() => {
      if (streaming) {
        res.end();
      } else if (!res.headersSent) {
        error(res, 504, "Gateway response timed out");
      } else {
        res.destroy();
      }
      request.destroy();
    }, lifetimeMs);
    res.once("close", () => {
      clearTimeout(deadline);
      request.destroy();
    });
    req.once("error", () => res.destroy());
    request.once("error", () => {
      if (res.writableEnded || res.destroyed) return;
      if (res.headersSent) res.destroy();
      else error(res, 502, "Gateway upstream unavailable");
    });
    request.once("response", response => {
      const forwarded = endToEnd(response.headers);
      delete forwarded["cdn-cache-control"];
      delete forwarded["vercel-cdn-cache-control"];
      forwarded["cache-control"] = "private, no-store";
      streaming = forwarded["content-type"]?.split(";", 1)[0].trim() === "text/event-stream";
      if (streaming) delete forwarded["content-length"];
      res.writeHead(response.statusCode, forwarded);
      if (streaming) res.flushHeaders();
      response.once("error", () => {
        if (!res.writableEnded) res.destroy();
      });
      response.pipe(res);
    });
    req.pipe(request);
  };
}

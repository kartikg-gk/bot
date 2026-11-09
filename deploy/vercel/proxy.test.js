import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import { once } from "node:events";

import app from "./app.js";
import { gatewayProxy } from "./proxy.js";

async function listen(t, server) {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => {
    server.closeAllConnections();
    return new Promise(resolve => server.close(resolve));
  });
  return "http://127.0.0.1:" + server.address().port;
}

async function fixture(t, handler, { lifetime, entrypoint } = {}) {
  const upstream = await listen(t, http.createServer(handler));
  const env = {
    DELTA_GATEWAY_UPSTREAM_URL: upstream,
    DELTA_GATEWAY_PROXY_SECRET: "x".repeat(43),
  };
  const proxy = await listen(t, http.createServer(entrypoint ? app : gatewayProxy(env, lifetime)));
  env.DELTA_GATEWAY_PUBLIC_ORIGIN = proxy;
  if (entrypoint) {
    const previous = Object.fromEntries(Object.keys(env).map(name => [name, process.env[name]]));
    Object.assign(process.env, env);
    t.after(() => {
      for (const [name, value] of Object.entries(previous)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    });
  }
  return { proxy, upstream, env };
}

test("the entrypoint forwards the original route, query, method, body and browser auth", async t => {
  const { proxy, upstream, env } = await fixture(t, async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    res.end(JSON.stringify({
      url: req.url, method: req.method, headers: req.headers,
      body: Buffer.concat(chunks).toString(),
    }));
  }, { entrypoint: true });
  const response = await fetch(proxy + "/api/runs?after=1&after=2", {
    method: "POST",
    headers: {
      "authorization": "Bearer browser-token", "cookie": "delta_session=example",
      "origin": proxy, "content-type": "application/json",
      "x-delta-gateway-proxy": "forged", "x-forwarded-host": "forged.test",
    },
    body: '{ "text": "hello" }',
  });
  const request = await response.json();
  assert.equal(request.url, "/api/runs?after=1&after=2");
  assert.equal(request.method, "POST");
  assert.equal(request.body, '{ "text": "hello" }');
  assert.equal(request.headers.origin, proxy);
  assert.equal(request.headers.host, new URL(upstream).host);
  assert.equal(request.headers.authorization, "Bearer browser-token");
  assert.equal(request.headers.cookie, "delta_session=example");
  assert.equal(request.headers["x-delta-gateway-proxy"], env.DELTA_GATEWAY_PROXY_SECRET);
  assert.equal(request.headers["x-forwarded-host"], undefined);
});

test("OAuth redirects preserve independent Set-Cookie headers and disable caching", async t => {
  const { proxy } = await fixture(t, (req, res) => {
    assert.equal(req.url, "/auth/callback?state=one%2Btwo&code=a%2Fb");
    res.writeHead(302, {
      location: "/",
      "set-cookie": [
        "delta_login=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/",
        "delta_session=test; HttpOnly; SameSite=Lax; Secure; Path=/",
      ],
      "cdn-cache-control": "public, max-age=100",
      "vercel-cdn-cache-control": "public, max-age=100",
    });
    res.end();
  });
  const response = await fetch(proxy + "/auth/callback?state=one%2Btwo&code=a%2Fb",
    { redirect: "manual" });
  assert.equal(response.status, 302);
  assert.equal(response.headers.get("location"), "/");
  assert.equal(response.headers.getSetCookie().length, 2);
  assert.match(response.headers.getSetCookie()[1], /HttpOnly.*Secure/);
  assert.equal(response.headers.get("cache-control"), "private, no-store");
  assert.equal(response.headers.get("cdn-cache-control"), null);
  assert.equal(response.headers.get("vercel-cdn-cache-control"), null);
});

test("SSE arrives before upstream EOF and reconnect retains its event cursor", async t => {
  const cursors = [];
  const closed = [];
  const { proxy } = await fixture(t, (req, res) => {
    cursors.push(req.headers["last-event-id"]);
    closed.push(once(res, "close"));
    res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8" });
    res.write("id: 42\ndata: ready\n\n");
  }, { lifetime: 350 });
  const endpoint = proxy + "/api/runs/test/events";
  const response = await fetch(endpoint, { signal: AbortSignal.timeout(2000) });
  const reader = response.body.getReader();
  const first = await reader.read();
  assert.match(Buffer.from(first.value).toString(), /id: 42/);
  assert.equal((await reader.read()).done, true);
  await closed[0];
  const controller = new AbortController();
  const replay = await fetch(endpoint, {
    headers: { "Last-Event-ID": "42" },
    signal: controller.signal,
  });
  assert.equal(replay.status, 200);
  controller.abort();
  await closed[1];
  assert.deepEqual(cursors, [undefined, "42"]);
});

test("unknown hosts, off-route paths and invalid origins fail before contacting upstream", async t => {
  let contacted = 0;
  const { proxy, env } = await fixture(t, (req, res) => {
    contacted++;
    res.end();
  });
  assert.equal((await fetch(proxy + "/internal")).status, 404);
  assert.equal((await fetch(proxy + "/api/%252e%252e/internal")).status, 404);
  env.DELTA_GATEWAY_PUBLIC_ORIGIN = "https://other.example";
  assert.equal((await fetch(proxy)).status, 421);
  env.DELTA_GATEWAY_UPSTREAM_URL = "http://upstream.example";
  assert.equal((await fetch(proxy)).status, 503);
  assert.equal(contacted, 0);
});

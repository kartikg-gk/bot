"""One-shot local operator tasks using the gateway's normal bot runtime."""

import asyncio
import json
import sys

from delta_gateway.budget import operation_scope
from delta_gateway.instances import OperatorError, config_lease
from delta_gateway.run_usage import RunUsage
from delta_gateway.service import Service
from delta_gateway.tracing import traced
from delta_gateway.web import WEB_CHAT, WebChannel, WebServer


async def run_task(config, text, *, bot_name=None, account=None, deadline=120, factory=None):
    if not isinstance(text, str) or not text.strip() or len(text) > 16000:
        raise OperatorError("Provide task text up to 16000 characters")
    text = text.strip()
    if text.startswith("/") and not text.startswith("/skill:"):
        raise OperatorError("Provide task text or a skill invocation")
    if not 0 < deadline <= 3600:
        raise OperatorError("Task deadline must be between zero and 3600 seconds")
    try:
        lease = config_lease(config)
    except RuntimeError:
        raise OperatorError("Stop this gateway before running a local one-shot task") from None
    with lease:
        root = Service(config, WebChannel(None), factory=factory)
        try:
            service = root
            if config.web_auth == "google":
                if not account:
                    raise OperatorError("Select a signed-in account with --account EMAIL")
                root.web = WebServer(root, root.telegram, host=config.web_host)
                user = next(
                    (
                        entry
                        for entry in root.web.accounts.all_accounts()
                        if entry["email"].casefold() == account.casefold()
                    ),
                    None,
                )
                if user is None or not root.web.google.permitted(user["email"], user["domain"]):
                    raise OperatorError("No admitted account matches that email")
                service = root.web.runtimes.get(user)
            elif account:
                raise OperatorError("--account is available in Google sign-in mode")
            return await execute(service, text, bot_name, deadline)
        finally:
            await root.close()


@traced("one-shot")
async def execute(service, text, bot_name, deadline):
    store = service.store
    bot = (store.bot(bot_name) or store.find_bot(bot_name)) if bot_name else store.bots()[0]
    if bot is None or bot.temporary:
        raise OperatorError("Unknown persistent bot")
    actor = service.config.users[0]
    route = store.conversation(WEB_CHAT, actor)
    calls = set()
    usage = RunUsage()

    async def delegated(caller, client, event):
        usage.observe(event)
        if event.get("type") == "host_tool_call":
            call = asyncio.create_task(
                service.host_call(client, event, caller, route, actor, delegated)
            )
            calls.add(call)
            call.add_done_callback(calls.discard)

    async def on_event(client, event):
        await delegated(bot, client, event)

    try:
        with operation_scope(service.config.instance) as budget:
            async with asyncio.timeout(deadline):
                answer, status = await service.sessions.delegate(
                    bot, text, on_event, review_durable=False
                )
    except TimeoutError:
        answer, status = "Task timed out", "timed_out"
    finally:
        for call in calls:
            call.cancel()
        await asyncio.gather(*calls, return_exceptions=True)
    result = {"bot": bot.id, "status": status, "answer": answer, "usage": usage.result()}
    if service.config.spend_metered:
        result["usage"]["reserved_micros"] = budget.spend_micros
    return result


async def command(config, options):
    text = sys.stdin.read(16001) if options.text == "-" else options.text
    result = await run_task(
        config, text, bot_name=options.bot, account=options.account, deadline=options.deadline
    )
    if options.json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(result["answer"])
        usage = result["usage"]
        print(
            f"{result['status']} | input {usage['input']} | output {usage['output']} | "
            f"cache read {usage['cache_read']} | cache write {usage['cache_write']} | "
            f"reported cost ${usage['cost_usd']:.6f}",
            file=sys.stderr,
        )
        if "reserved_micros" in usage:
            print(f"Reserved spend ${usage['reserved_micros'] / 1_000_000:.6f}", file=sys.stderr)
    if result["status"] != "completed":
        raise SystemExit(1)

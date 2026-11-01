"""Session and model operations at the gateway's adapter boundary."""

import html
import inspect
import json
import os
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from delta_gateway.ownership import SessionLock


class ControlError(ValueError):
    """A user-facing, credential-free explanation of a rejected operation."""


def require_text(arguments, key, *, limit=16000, optional=False):
    value = arguments.get(key, "" if optional else None)
    if not isinstance(value, str) or len(value) > limit or (not optional and not value.strip()):
        raise ControlError(f"{key} must be text between 1 and {limit} characters")
    return value.strip()


def snapshot(session):
    from delta_app.catalog import model_info

    usage = asdict(session.usage)
    facts = model_info(session.provider_name, session.model)
    usage["cost_available"] = bool((facts and facts.pricing) or usage["total_cost_usd"])
    return dict(
        session_id=session.session_id,
        title=session.title,
        provider=session.provider_name,
        model=session.model,
        thinking=session.thinking_level or "off",
        generating=session.active,
        pending=session.pending.count,
        usage=usage,
    )


def list_models(provider="", query="", offset=0):
    from delta_app.catalog import available_providers, model_info, models_for

    rows = []
    for info in available_providers():
        if provider and info.key != provider:
            continue
        for model in models_for(info.key):
            label = f"{info.key}/{model}"
            if query.lower() not in label.lower():
                continue
            facts = model_info(info.key, model)
            rows.append(dict(id=label, reasoning=facts.reasoning if facts else None))
    return dict(
        models=rows[offset : offset + 30],
        total=len(rows),
        offset=offset,
        next_offset=offset + 30 if offset + 30 < len(rows) else None,
    )


def prepare_provider(provider, original_environment):
    from delta_app.catalog import lookup_provider
    from delta_app.config.store import load_config

    if provider == "scripted":
        return
    try:
        info = lookup_provider(provider)
    except KeyError:
        raise ControlError("Unknown provider; use /models") from None
    config = load_config()
    if info.key_env:
        key = original_environment.get(info.key_env) or config.api_keys.get(provider)
        if not key and info.needs_key:
            raise ControlError("Provider has no configured key; configure it locally first")
        prefix = "ANTHROPIC" if provider == "anthropic" else "OPENAI"
        os.environ[prefix + "_API_KEY"] = key or provider
        native_url = original_environment.get(provider.upper() + "_BASE_URL")
        os.environ[prefix + "_BASE_URL"] = (
            native_url or config.base_urls.get(provider) or info.base_url
        )


async def change_model(session, selection, original_environment):
    from delta_app.config.config import resolve_provider

    provider, separator, model = selection.partition("/")
    if not separator:
        model, provider = provider, session.provider_name
    if not provider or not model or any(c.isspace() for c in selection):
        raise ControlError("Use /model provider/model-id or /model model-id")
    if provider == session.provider_name:
        await session.switch_model(model)
        return snapshot(session)
    prepare_provider(provider, original_environment)
    try:
        replacement = resolve_provider(provider)
    except Exception:
        raise ControlError(
            "Provider setup failed; configure its credentials locally first"
        ) from None
    previous = session.harness.settings.provider
    try:
        await session.switch_provider(replacement, provider, model)
    except BaseException:
        if replacement is not session.harness.settings.provider:
            close = getattr(replacement, "close", None)
            if close:
                result = close()
                if inspect.isawaitable(result):
                    await result
        raise
    close = getattr(previous, "close", None)
    if close:
        result = close()
        if inspect.isawaitable(result):
            await result
    return snapshot(session)


async def change_thinking(session, level):
    from delta_app.catalog import model_info
    from delta_app.reasoning import THINKING_LEVELS

    requested = level.lower()
    effective = "xhigh" if requested == "max" else requested
    if effective not in THINKING_LEVELS:
        raise ControlError("Use off, minimal, low, medium, high, xhigh, or max (alias for xhigh)")
    facts = model_info(session.provider_name, session.model)
    if effective != "off" and (facts and facts.reasoning is False):
        raise ControlError("This model does not support thinking controls")
    if effective != "off" and not hasattr(session.harness.settings.provider, "set_reasoning"):
        raise ControlError("This provider does not expose thinking controls")
    await session.set_thinking(None if effective == "off" else effective)
    return dict(**snapshot(session), requested_thinking=requested)


async def compact(session, instructions):
    # The current Delta API lacks an instructions argument. Scope the extra summary
    # instruction to this one utility call, with restoration even on cancellation.
    original = session._utility_completion

    async def summarize(prompt, system):
        if instructions:
            prompt += "\n\nAdditional summary instructions from the user:\n" + instructions
        return await original(prompt, system)

    session._utility_completion = summarize
    try:
        summary = await session.compact()
    except ValueError:
        raise ControlError("There is not enough conversation history to compact yet") from None
    finally:
        session._utility_completion = original
    return dict(message="Conversation compacted", summary=summary.summary, **snapshot(session))


async def clone(session, directory):
    from delta_harness.session.records import SessionMetaRecord, TagRecord, TipRecord
    from delta_harness.session.replay import find_tip, trace_to_entry
    from delta_harness.session.store import deserialize_records, serialize_record

    records = deserialize_records((await session.export("jsonl")).splitlines())
    tip = session._tip_id or find_tip(records)
    branch = trace_to_entry(records, tip) if tip else []
    if not branch:
        branch = [SessionMetaRecord(cwd=session.cwd)]
    # IDs only identify records within one session file. Keep branch links intact.
    title = ("Clone of " + (session.title or session.session_id))[:120]
    label = TagRecord(label=title, parent_id=branch[-1].id)
    branch.extend([label, TipRecord(entry_id=label.id)])
    session_id = uuid4().hex[:12]
    directory = Path(directory)
    lock = SessionLock(directory / "locks", session_id)
    path = directory / f"{session_id}.jsonl"
    created = False
    try:
        with path.open("x", encoding="utf-8", newline="\n") as target:
            created = True
            target.write("".join(serialize_record(record) for record in branch))
            target.flush()
            os.fsync(target.fileno())
        path.chmod(0o600)
    except BaseException:
        if created:
            path.unlink(missing_ok=True)
        raise
    finally:
        lock.close()
    return dict(
        session_id=session_id,
        title=title,
        provider=session.provider_name,
        model=session.model,
        thinking=session.thinking_level or "off",
    )


def html_export(session):
    from delta_harness.contracts.transcript import surface_text

    title = html.escape(session.title or session.session_id)
    parts = [
        "<!doctype html><html><head><meta charset='utf-8'>",
        "<meta http-equiv='Content-Security-Policy' "
        "content=\"default-src 'none'; style-src 'unsafe-inline'\">",
        f"<title>{title}</title><style>"
        "body{max-width:900px;margin:2em auto;font-family:system-ui}",
        ("pre{white-space:pre-wrap;overflow-wrap:anywhere}article{border-bottom:1px solid #aaa}"),
        f"</style></head><body><h1>{title}</h1>",
    ]
    for entry in session.transcript:
        text = html.escape(surface_text(entry))
        parts.append(f"<article><h2>{html.escape(entry.role)}</h2><pre>{text}</pre></article>")
    parts.append("</body></html>")
    content = "".join(parts)
    if len(json.dumps(content, ensure_ascii=True)) > 12 * 1024 * 1024:
        raise ControlError("Export is too large for the worker protocol")
    return dict(
        filename=f"delta-{session.session_id}.html",
        content=content,
        content_type="text/html; charset=utf-8",
    )

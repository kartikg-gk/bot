"""Model request limits shared by embedded sessions in one gateway instance."""

import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import wraps

from delta_harness.provider.wire import SourceRetryEvent


@dataclass
class RunBudget:
    requests: int = 0
    spend_micros: int = 0
    refusal: str | None = None


operation = ContextVar("gateway_operation_budget", default=None)


@contextmanager
def operation_scope(instance):
    run = RunBudget()
    token = operation.set((instance, run))
    try:
        yield run
    finally:
        operation.reset(token)


def budgeted(method):
    @wraps(method)
    async def wrapped(service, *args, **kwargs):
        with operation_scope(service.config.instance):
            return await method(service, *args, **kwargs)

    return wrapped


class ModelBudget:
    def __init__(self, config, store):
        self.config = config
        self.store = store
        self.run = ContextVar("model_request_budget", default=None)

    def scope(self):
        scoped = operation.get()
        return scoped[1] if scoped and scoped[0] == self.config.instance else RunBudget()

    async def reserve_http(self, provider, request, *, run=None):
        run = run if run is not None else self.run.get()
        try:
            model = json.loads(request.content)["model"]
        except (KeyError, TypeError, ValueError):
            model = None
        maximum = self.config.model_attempt_max_micros.get(f"{provider}/{model}")
        if not isinstance(model, str) or maximum is None or not 0 <= maximum <= 10**12:
            self.refuse(run, "No approved maximum charge is configured for this model.")
        await self.reserve(run=run, micros=maximum)

    @staticmethod
    def refuse(run, reason):
        if run is not None:
            run.refusal = reason
        raise RuntimeError(reason)

    async def reserve(self, *, run=None, micros=None):
        run = run if run is not None else self.run.get()
        if run is None:
            raise RuntimeError("Model request outside a metered gateway operation")
        if run.refusal:
            raise RuntimeError(run.refusal)
        if run.requests >= self.config.model_requests_per_run:
            self.refuse(run, "Model request limit for this run reached.")
        per_run = self.config.model_spend_micros_per_run
        per_day = self.config.model_spend_micros_per_day
        if micros is not None and per_run is not None and run.spend_micros + micros > per_run:
            self.refuse(run, "Model spend limit for this run reached.")
        day = datetime.now(UTC).date().isoformat()
        # Each limit is checked in the same statement that counts against it.
        updated = await self.store.shared.query(
            "INSERT INTO model_requests(instance,day,count) VALUES(?,?,1) "
            "ON CONFLICT(instance,day) DO UPDATE SET count=count+1 WHERE count<? "
            "RETURNING count",
            (self.config.instance, day, self.config.model_requests_per_day),
        )
        if not updated:
            self.refuse(run, "Daily model request limit reached. It resets at midnight UTC.")
        if micros is not None:
            limit = per_day or 2**62
            debit = await self.store.shared.query(
                "INSERT INTO model_spend(instance,day,micros) SELECT ?,?,? "
                "WHERE CAST(? AS INTEGER)<=CAST(? AS INTEGER) "
                "ON CONFLICT(instance,day) DO UPDATE SET micros=micros+excluded.micros "
                "WHERE micros<=CAST(? AS INTEGER)-excluded.micros RETURNING micros",
                (self.config.instance, day, micros, micros, limit, limit),
            )
            if not debit:
                # A refused request does not count, as when both ran in one transaction.
                await self.store.shared.run(
                    "UPDATE model_requests SET count=count-1 WHERE instance=? AND day=?",
                    (self.config.instance, day),
                )
                self.refuse(run, "Daily model spend limit reached. It resets at midnight UTC.")
        run.requests += 1
        run.spend_micros += micros or 0

    def wrap(self, provider, name):
        return MeteredProvider(provider, self, name)


class MeteredProvider:
    def __init__(self, provider, budget, name):
        self.provider = provider
        spend_metered = budget.config.spend_metered
        hook = getattr(provider, "add_model_request_hook", None)
        if spend_metered and hook is not None:

            async def before_send(request):
                await budget.reserve_http(name, request)

            hook(before_send)

        @wraps(provider.stream_response)
        def stream_response(*args, **kwargs):
            if spend_metered:
                if hook is None:
                    budget.refuse(budget.run.get(), "This provider does not support spend limits.")
                return provider.stream_response(*args, **kwargs)
            return self.metered(provider, args, kwargs, budget)

        self.stream_response = stream_response

    @staticmethod
    async def metered(provider, args, kwargs, budget):
        # The first request is reserved before the provider is called at all.
        await budget.reserve()
        events = provider.stream_response(*args, **kwargs)
        try:
            async for event in events:
                if isinstance(event, SourceRetryEvent):
                    await budget.reserve()
                yield event
        finally:
            if close := getattr(events, "aclose", None):
                await close()

    def __getattr__(self, name):
        return getattr(self.provider, name)


def limited_server(base, budget):
    class LimitedServer(base):
        def __init__(self, session, *args, **kwargs):
            super().__init__(session, *args, **kwargs)
            self.meter_provider_changes()

        def meter_provider_changes(self):
            original = self.session.switch_provider

            async def switch(provider, provider_name, *args, **kwargs):
                return await original(
                    budget.wrap(provider, provider_name), provider_name, *args, **kwargs
                )

            self.session.switch_provider = switch

        async def change_session(self, **kwargs):
            result = await super().change_session(**kwargs)
            self.meter_provider_changes()
            return result

        def launch(self, request, action, *, prompt=False):
            # The task created by launch inherits this operation's context.
            token = budget.run.set(budget.scope())
            try:
                super().launch(request, action, prompt=prompt)
            finally:
                budget.run.reset(token)

        def send(self, record):
            run = budget.run.get()
            if run is not None and run.refusal:
                if record.get("type") == "agent_settled":
                    record = {**record, "outcome": "failed", "error": run.refusal}
                elif record.get("type") == "response" and not record.get("success"):
                    record = {**record, "error": run.refusal}
            super().send(record)

    return LimitedServer

"""Common Google API request helpers."""

from urllib.parse import quote


def segment(value):
    if not value or value in {".", ".."}:
        raise ValueError("Invalid Google resource ID")
    return quote(value, safe="")


async def request(http, headers, method, url, **kwargs):
    response = await http.request(method, url, headers=headers, **kwargs)
    response.raise_for_status()
    return response.json() if response.content else None

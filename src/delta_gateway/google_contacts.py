"""Read-only contact search for the connected Google account."""

from delta_gateway.google_api import request
from delta_gateway.plugin_schema import TEXT, tool

PEOPLE_API = "https://people.googleapis.com/v1/people:searchContacts"
CONTACT_TOOLS = [
    tool(
        "contacts_search",
        "Find Google contacts by name, email, phone or organization.",
        {"query": TEXT, "limit": {"type": "integer", "minimum": 1, "maximum": 30}},
        ["query"],
    ),
]


async def contacts(http, headers, _name, args):
    query = args["query"].strip()
    if not query:
        raise ValueError("Provide a non-empty contact search query")
    fields = {
        "names": "displayName",
        "nicknames": "value",
        "emailAddresses": "value",
        "phoneNumbers": "value",
        "organizations": "name",
    }
    params = {"readMask": ",".join(fields), "sources": "READ_SOURCE_TYPE_CONTACT"}
    await request(http, headers, "GET", PEOPLE_API, params={**params, "query": "", "pageSize": 1})
    result = await request(
        http,
        headers,
        "GET",
        PEOPLE_API,
        params={
            **params,
            "query": query,
            "pageSize": args.get("limit", 10),
        },
    )
    found = []
    for item in result.get("results", []):
        person = item["person"]
        record = {
            name: [entry[field] for entry in person.get(name, []) if entry.get(field)]
            for name, field in fields.items()
        }
        found.append({"id": person.get("resourceName"), **record})
    return {"contacts": found}

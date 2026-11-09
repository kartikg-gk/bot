import json

import httpx
import pytest

from delta_gateway.plugins import UNTRUSTED, PluginRunner


async def test_google_contacts_warms_cache_and_searches_connected_address_book():
    queries = []

    async def token(app):
        assert app == "contacts"
        return "contact-token"

    def provider(req):
        assert req.url.host == "people.googleapis.com"
        assert req.url.path == "/v1/people:searchContacts"
        assert req.headers["authorization"] == "Bearer contact-token"
        assert req.url.params["sources"] == "READ_SOURCE_TYPE_CONTACT"
        assert "phoneNumbers" in req.url.params["readMask"]
        queries.append((req.url.params["query"], req.url.params["pageSize"]))
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "person": {
                            "resourceName": "people/c1",
                            "names": [{"displayName": "Ada Example"}],
                            "emailAddresses": [
                                {"value": "ada@example.test", "metadata": {"private": True}}
                            ],
                            "phoneNumbers": [{"value": "555-0100"}],
                            "organizations": [{"name": "Example"}],
                        }
                    }
                ],
            },
        )

    client = PluginRunner(transport=httpx.MockTransport(provider), access_token=token)
    try:
        output = await client.run("contacts_search", {"query": " Ada ", "limit": 4})
        contact = json.loads(output.removeprefix(UNTRUSTED))["contacts"][0]
        assert contact == {
            "id": "people/c1",
            "names": ["Ada Example"],
            "nicknames": [],
            "emailAddresses": ["ada@example.test"],
            "phoneNumbers": ["555-0100"],
            "organizations": ["Example"],
        }
        with pytest.raises(ValueError, match="non-empty"):
            await client.run("contacts_search", {"query": "   "})
        assert queries == [("", "1"), ("Ada", "4")]
    finally:
        await client.close()

"""JSON schemas for connected tools."""

TEXT = {"type": "string", "minLength": 1}
NUMBER = {"type": "integer", "minimum": 1}
OFFSET = {"type": "integer", "minimum": 0}
STRING_LIST = {"type": "array", "items": TEXT, "maxItems": 100}


def tool(name, description, properties, required=None, *, write=False):
    definition = dict(
        name=name,
        description=description,
        parameters={
            "type": "object",
            "properties": properties,
            "required": list(properties) if required is None else required,
            "additionalProperties": False,
        },
    )
    if not write:
        definition["approval"] = False
    return definition

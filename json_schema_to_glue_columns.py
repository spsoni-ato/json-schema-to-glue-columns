import json

def glue_column_type_from_json_schema(value):
    """
    Converts a JSON schema type to an AWS Glue column type.

    Args:
        value (dict): The JSON schema type.

    Returns:
        str: The corresponding AWS Glue column type.

    Raises:
        Exception: If the JSON schema type is unknown or an empty array is encountered.
    """
    if value["type"] == "string":
        return "STRING"
    elif value["type"] == "integer":
        return "BIGINT"
    elif value["type"] == "number":
        return "DOUBLE"
    elif value["type"] == "boolean":
        return "BOOLEAN"
    elif value["type"] == "object":
        _columns = []

        # Convert each property of the object to Glue column type
        for _key, _value in value["properties"].items():
            _column_type = glue_column_type_from_json_schema(_value)
            _column = f"{_key}:{_column_type}"
            _columns.append(_column)
        
        return f"STRUCT<{','.join(_columns)}>"
    elif value["type"] == "array":
        items = value["items"]

        if len(items) == 0:
            raise Exception("Empty arrays are not allowed in Glue")

        if items["type"] == "array":
            return f"ARRAY<{glue_column_type_from_json_schema(items)}>[]"
        else:
            return f"ARRAY<{glue_column_type_from_json_schema(items)}>"
    else:
        raise Exception("Unknown type")

def convert_json_schema_to_glue_columns(json_schema):
    """
    Converts a JSON schema to a list of AWS Glue columns.

    Args:
        json_schema (dict): The JSON schema.

    Returns:
        list: The list of Glue columns.
    """
    columns = []

    # Convert each property of the JSON schema to a Glue column
    for key, value in json_schema["properties"].items():
        column = {
            "Name": key,
            "Type": glue_column_type_from_json_schema(value)
        }
    
        columns.append(column)

    return columns

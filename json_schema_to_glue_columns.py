import json

import boto3
import yaml

def load_json_schema(schema_file_location):
    """
    Loads a JSON schema from a file.

    Args:
        schema_file_location (str): The path to the JSON schema file.

    Returns:
        dict: The loaded JSON schema.
    """
    # Open the JSON schema file
    with open(schema_file_location) as schema_file:
        # Load the JSON schema
        schema = json.load(schema_file)

    return schema


def load_yaml_config(config_file_location):
    """
    Loads a configuration file in YAML format.

    Args:
        config_file_location (str): The path to the configuration file.

    Returns:
        dict: The loaded configuration data.
    """
    # Open the configuration file
    with open(config_file_location) as config_file:
        # Load the YAML content
        config = yaml.safe_load(config_file)

    return config


def load_json_from_s3(bucket_name, object_key):
    """
    Retrieves a JSON file from an S3 bucket and loads its contents.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the JSON file within the bucket.

    Returns:
        dict: The loaded JSON data.
    """
    # Create an S3 client
    s3 = boto3.client('s3')

    # Retrieve the object from S3
    response = s3.get_object(Bucket=bucket_name, Key=object_key)

    # Read the JSON content from the response
    json_content = response['Body'].read().decode('utf-8')

    # Parse the JSON content
    data = json.loads(json_content)

    return data


def load_yaml_from_s3(bucket_name, object_key):
    """
    Retrieves a YAML file from an S3 bucket and loads its contents.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the YAML file within the bucket.

    Returns:
        dict: The loaded YAML data.
    """
    # Create an S3 client
    s3 = boto3.client('s3')

    # Retrieve the object from S3
    response = s3.get_object(Bucket=bucket_name, Key=object_key)

    # Read the YAML content from the response
    yaml_content = response['Body'].read().decode('utf-8')

    # Parse the YAML content
    yaml_data = yaml.safe_load(yaml_content)

    return yaml_data


def flatten_json_schema(json_schema, prefix='', delimiter='_'):
    """
    Flattens a JSON schema by unwrapping nested structs and creating flattened property names.

    Args:
        json_schema (dict): The JSON schema.
        prefix (str): Prefix for the flattened property names (used for recursion).
        delimiter (str): Delimiter to separate flattened property names.

    Returns:
        dict: The flattened JSON schema.
    """
    flattened_schema = {}

    for key, value in json_schema["properties"].items():
        if value["type"] == "object":
            # Flatten the nested struct recursively
            flattened_properties = flatten_json_schema(value, prefix=f"{prefix}{key}{delimiter}", delimiter=delimiter)
            flattened_schema.update(flattened_properties)
        else:
            # Create the flattened property name
            flattened_key = f"{prefix}{key}"
            flattened_schema[flattened_key] = value

    return flattened_schema


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


def convert_json_schema_to_glue_columns(json_schema, flatten=False, delimiter='_'):
    """
    Converts a JSON schema to a list of AWS Glue columns.

    Args:
        json_schema (dict): The JSON schema.
        flatten (bool): Whether to flatten the schema before generating Glue columns.
        delimiter (str): Delimiter to separate flattened property names.

    Returns:
        list: The list of Glue columns.
    """

    if "properties" not in json_schema:
        raise Exception("Required key 'properties' not found in the JSON schema.")

    if flatten:
        flattened_schema = flatten_json_schema(json_schema, delimiter=delimiter)
        schema_properties = flattened_schema
    else:
        schema_properties = json_schema["properties"]

    columns = []

    for key, value in schema_properties.items():
        if "type" not in value:
            raise Exception(f"Required key 'type' not found for {key}.")

        column = {
            "Name": key,
            "Type": glue_column_type_from_json_schema(value)
        }
    
        columns.append(column)

    return columns

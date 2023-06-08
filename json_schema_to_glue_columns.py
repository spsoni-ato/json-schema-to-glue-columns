import json
import re

import boto3
import yaml

def replace_placeholders(dictionary, placeholders):
    """
    Replaces placeholders in a dictionary with corresponding values using regex.

    Args:
        dictionary (dict): The dictionary to process.
        placeholders (dict): A dictionary of placeholders and their corresponding values.

    Returns:
        dict: The updated dictionary with replaced placeholders.
    """
    replaced_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, dict):
            # Recursively process nested dictionaries
            replaced_dict[key] = replace_placeholders(value, placeholders)
        elif isinstance(value, str):
            # Replace placeholders in string values using regex
            pattern = re.compile(r'\[(.*?)\]')
            replaced_value = pattern.sub(lambda x: placeholders[x.group()], value)
            replaced_dict[key] = replaced_value
        else:
            # Keep non-string, non-dict values as is
            replaced_dict[key] = value
    return replaced_dict


def convert_to_lower_with_underscore(string):
    """
    Converts a string to lowercase, replaces special characters with an underscore,
    and strips leading/trailing whitespaces.

    Args:
        string (str): The input string.

    Returns:
        str: The converted string.
    """
    # Strip leading/trailing whitespaces
    stripped_string = string.strip()

    # Convert the string to lowercase
    lowercase_string = stripped_string.lower()

    # Replace special characters with an underscore
    converted_string = re.sub(r'[^a-z0-9_]+', '_', lowercase_string)

    return converted_string


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


def load_yaml_config(config_file_location, placeholders):
    """
    Loads a configuration file in YAML format.

    Args:
        config_file_location (str): The path to the configuration file.
        placeholders (dict): A dictionary of placeholders and their corresponding values.

    Returns:
        dict: The loaded configuration data.
    """
    # Open the configuration file
    with open(config_file_location) as config_file:
        # Load the YAML content
        _config = yaml.safe_load(config_file)
        config = replace_placeholders(_config, placeholders)

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


def create_partition_keys(partition_key_list):
    """
    Creates a list of partition keys from a given partition key list.

    Args:
        partition_key_list (list): A list of partition keys in the format "key_name:key_type".

    Returns:
        list: A list of partition keys as dictionaries, with each dictionary containing "Name" and "Type" keys.

    Example:
        Input: ["year:int", "month:string"]
        Output: [{"Name": "year", "Type": "int"}, {"Name": "month", "Type": "string"}]
    """
    partition_keys = []
    for partition_key in partition_key_list:
        # Split the partition key into name and type
        key_name, key_type = partition_key.split(":")

        # Create a dictionary for the partition key
        partition_key_dict = {
            "Name": key_name.strip(),
            "Type": key_type.strip()
        }

        # Add the partition key to the list
        partition_keys.append(partition_key_dict)

    return partition_keys


def get_additional_columns(pipeline_type):
    """
    Get additional columns based on the pipeline type.

    Args:
        pipeline_type (str): The pipeline type.

    Returns:
        list or None: The additional columns as a list of dictionaries, or None if the pipeline type is unknown.
    """
    if pipeline_type.lower() == "scd1":
        # Additional columns for SCD1 pipeline type
        additional_columns = [{
            "Name": "last_updated",
            "Type": "STRING"
        }]
    elif pipeline_type.lower() == "scd2":
        # Additional columns for SCD2 pipeline type
        additional_columns = [{
            "Name": "last_updated",
            "Type": "STRING"
        },
        {
            "Name": "active",
            "Type": "STRING"
        }]
    else:
        # Unknown pipeline type, return None
        additional_columns = None
    
    return additional_columns


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
            if "type" not in _value:
                raise Exception(f"Required key 'type' not found for {_key}.")

            _key = convert_to_lower_with_underscore(_key)
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
            "Name": convert_to_lower_with_underscore(key),
            "Type": glue_column_type_from_json_schema(value)
        }
    
        columns.append(column)

    return columns

class GlueTableManager:
    def __init__(self, database_name, table_name, columns, location, input_format, output_format, serde_info, partition_keys=None, table_type='EXTERNAL_TABLE', description='', parameters=None):
        """
        Initialize the GlueTableManager.

        Args:
            database_name (str): Name of the database.
            table_name (str): Name of the table.
            columns (list): List of column definitions.
            location (str): S3 location of the table data.
            input_format (str): Input format of the table.
            output_format (str): Output format of the table.
            serde_info (dict): Serde information for the table.
            partition_keys (list, optional): List of partition key definitions. Defaults to None.
            table_type (str, optional): Type of the table. Defaults to 'EXTERNAL_TABLE'.
            description (str, optional): Description of the table. Defaults to ''.
            parameters (dict, optional): Additional parameters for the table. Defaults to None.
        """
        self.glue_client = boto3.client('glue')
        self.table_type = table_type
        self.description = description
        self.database_name = database_name
        self.table_name = table_name
        self.columns = columns
        self.location = location
        self.input_format = input_format
        self.output_format = output_format
        self.serde_info = serde_info
        self.partition_keys = partition_keys if partition_keys else []
        self.parameters = parameters if parameters else {}

    def create_database_if_not_exist(self):
        """
        Creates the database if it does not exist.
        """
        try:
            # Attempt to retrieve the database
            self.glue_client.get_database(Name=self.database_name)
        except self.glue_client.exceptions.EntityNotFoundException:
            # Database does not exist, create it
            self.glue_client.create_database(DatabaseInput={'Name': self.database_name})

    def create_or_update_table(self):
        """
        Creates or updates a table in AWS Glue.

        Returns:
            dict: The response from the Glue API.
        """
        self.create_database_if_not_exist()

        table_input = {
            'Name': self.table_name,
            'Description': self.description,
            'StorageDescriptor': {
                'Columns': self.columns,
                'Location': self.location,
                'InputFormat': self.input_format,
                'OutputFormat': self.output_format,
                'SerdeInfo': self.serde_info,
            },
            'PartitionKeys': self.partition_keys,
            'TableType': self.table_type,
            'Parameters': self.parameters
        }

        try:
            # Try to create the table
            response = self.glue_client.create_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
        except self.glue_client.exceptions.AlreadyExistsException:
            # Table already exists, update it instead
            response = self.glue_client.update_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
        return response
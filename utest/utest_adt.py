import os
from azure.identity import DefaultAzureCredential
from azure.digitaltwins.core import DigitalTwinsClient


# configs
digital_twin_id_1 = 'Room0'
digital_twin_id_2 = 'Floor0'
digital_twin_id_3 = 'Room2'
model_id = 'dtmi:example:Floor;1'

# TODO: add `AZURE_ADT_URL`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` to environment variable
AZURE_ADT_URL = os.getenv('AZURE_ADT_URL')

# connect to ADT
credential = DefaultAzureCredential()
adt_client = DigitalTwinsClient(AZURE_ADT_URL, credential)

# get DigitalTwins by query
query_expression = 'SELECT * FROM digitaltwins'
query_result = adt_client.query_twins(query_expression=query_expression)
print('DT by query:')
for twin in query_result:
    print(twin)

# get DigitalTwins by ID
digital_twin = adt_client.get_digital_twin(digital_twin_id=digital_twin_id_1)
print('DT by ID:')
print(digital_twin)
print('Temperature: ', digital_twin['Temperature'])

# get all ADT Model
models = adt_client.list_models()
print('List all Model:')
for model in models:
    print(model)

# get ADT Model by ID
print('Model by ID:')
model = adt_client.get_model(model_id=model_id)
print(model)

# get Relationship by DT-ID
print('Relationshio by DT-ID:')
print('digital_twin_id_1: ', digital_twin_id_1)
relationships = adt_client.list_relationships(digital_twin_id=digital_twin_id_1)  # have no relationship
for relationship in relationships:
    print(relationship)
print('Relationshio by DT-ID:')
print('digital_twin_id_2: ', digital_twin_id_2)
relationships = adt_client.list_relationships(digital_twin_id=digital_twin_id_2)
for relationship in relationships:
    print(relationship)

# upsert DigitalTwins
# note: Property name must match the DT-Model, can replace original `digital_twin_id`
print('Upsert new DigitalTwins:')
digital_twin_dtdl = {
        "$metadata": {
            "$model": 'dtmi:example:Room;1'
        },
        "Temperature": 68,
    }
new_dt = adt_client.upsert_digital_twin(digital_twin_id=digital_twin_id_3, digital_twin=digital_twin_dtdl)
print(new_dt)

# update DigitalTwins property
print('Update DigitalTwins:')
json_patch = [
    {
        "op": "add",
        "path": "/Humidity",
        "value": 20
    },
    {
        "op": "replace",
        "path": "/Temperature",
        "value": 42
    },
    {
        "op": "remove",
        "path": "/Temperature"
    }
]
adt_client.update_digital_twin(digital_twin_id=digital_twin_id_3, json_patch=json_patch)
update_dt = adt_client.get_digital_twin(digital_twin_id=digital_twin_id_3)
print(update_dt)

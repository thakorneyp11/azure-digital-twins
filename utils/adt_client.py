"""
`Azure Identity` library to authenticate the azure user
Pypi: https://pypi.org/project/azure-identity
Github: https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/identity/azure-identity

`Azure Digital Twins` library to interact with ADT
Pypi: https://pypi.org/project/azure-digitaltwins-core
Github: https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/digitaltwins/azure-digitaltwins-core

`DigitalTwinsClient` class
https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/digitaltwins/azure-digitaltwins-core/azure/digitaltwins/core/_digitaltwins_client.py

JSON Patch for interacting with ADT
http://jsonpatch.com
"""

import os
from typing import Dict, List, Any
from azure.identity import DefaultAzureCredential
from azure.digitaltwins.core import DigitalTwinsClient


class ADTClient:
    def __init__(self, endpoint: str, verbose: bool = False):
        # TODO: add `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` to environment variable
        if os.getenv('AZURE_TENANT_ID') is None:
            raise Exception('ADTClient: environment variable `AZURE_TENANT_ID` not found')
        if os.getenv('AZURE_CLIENT_ID') is None:
            raise Exception('ADTClient: environment variable `AZURE_CLIENT_ID` not found')
        if os.getenv('AZURE_CLIENT_SECRET') is None:
            raise Exception('ADTClient: environment variable `AZURE_CLIENT_SECRET` not found')

        self.endpoint = endpoint
        self.verbose = verbose
        self.credential = DefaultAzureCredential()
        self.client = DigitalTwinsClient(self.endpoint, self.credential)
        self.valid_model_ids = self.get_model_ids()

    def print_check_verbose(self, message: str) -> None:
        if self.verbose:
            print(message)

    def get_model_ids(self) -> List[str]:
        models = self.client.list_models()
        valid_model_ids = [model.id for model in models]
        return valid_model_ids

    def get_digital_twin(self, digital_twin_id: str, **kwargs) -> Dict[str, object]:
        digital_twin = self.client.get_digital_twin(digital_twin_id, **kwargs)
        return digital_twin

    def get_digital_twin_property(self, digital_twin_id: str,
                                  property_names: List[str]) -> Dict[str, Any]:
        digital_twin = self.client.get_digital_twin(digital_twin_id=digital_twin_id)
        property_values = dict()
        for property_name in property_names:
            property_values.update({str(property_name): digital_twin.get(property_name, None)})
        return property_values

    def get_all_digital_twin(self) -> List[Dict[str, Any]]:
        query_expression = 'SELECT * FROM digitaltwins'
        all_digital_twin = self.client.query_twins(query_expression=query_expression)
        return list(all_digital_twin)

    def upsert_digital_twin(self, digital_twin_id: str,
                            digital_twin_model: str,
                            digital_twin_property: Dict[str, Any]) -> Dict[str, object]:
        if str(digital_twin_model) not in self.valid_model_ids:
            raise Exception('ADTClient.upsert_digital_twin(): invalid `digital_twin_model` input')

        dtdl = {
            "$metadata": {"$model": str(digital_twin_model)}
        }
        for property_name in digital_twin_property:
            # TODO: check whether `property_name` is valid in DT-Model
            property_value = digital_twin_property[str(property_name)]
            dtdl.update({str(property_name): property_value})

        digital_twin = self.client.upsert_digital_twin(digital_twin_id=digital_twin_id, digital_twin=dtdl)
        self.print_check_verbose(f'upsert DigitalTwin {digital_twin_id}')
        return digital_twin

    def update_digital_twin(self, digital_twin_id: str,
                            patch_add: Dict[str, Any] = None,
                            patch_replace: Dict[str, Any] = None,
                            patch_remove: List[str] = None) -> None:
        """update property values in the `digital_twin_id` DT
        update order: add > replace > remove
        """
        # TODO: check whether each property is valid in DT-Model
        json_patch = list()
        if patch_add is not None:
            for patch_name in patch_add:
                json_patch.append({
                    "op": "add",
                    "path": f"/{patch_name}",
                    "value": patch_add[patch_name]
                })
        if patch_replace is not None:
            for patch_name in patch_replace:
                json_patch.append({
                    "op": "replace",
                    "path": f"/{patch_name}",
                    "value": patch_replace[patch_name]
                })
        if patch_remove is not None:
            for patch_name in patch_remove:
                json_patch.append({
                    "op": "remove",
                    "path": f"/{patch_name}",
                })
        self.client.update_digital_twin(digital_twin_id=digital_twin_id, json_patch=json_patch)
        self.print_check_verbose(f'update DigitalTwin {digital_twin_id} properties')

    def delete_digital_twins(self, digital_twin_ids: List[str]) -> None:
        all_digital_twin = self.get_all_digital_twin()
        all_digital_twin_ids = [dt['$dtId'] for dt in all_digital_twin]
        for digital_twin_id in digital_twin_ids:
            if digital_twin_id in all_digital_twin_ids:
                self.client.delete_digital_twin(digital_twin_id=digital_twin_id)
                self.print_check_verbose(f'delete DigitalTwin {digital_twin_id}')


if __name__ == '__main__':
    # TODO: add `AZURE_ADT_URL` to environment variable
    AZURE_ADT_URL = os.getenv('AZURE_ADT_URL')
    adt_client = ADTClient(endpoint=AZURE_ADT_URL, verbose=True)

    # get a DigitalTwin info
    digital_twin = adt_client.get_digital_twin(digital_twin_id='Room0')
    print(digital_twin)

    # get DigitalTwin property-values
    digital_twin_property = adt_client.get_digital_twin_property(digital_twin_id='Room0',
                                                                 property_names=['Temperature', 'Humidity', 'Pressure'])
    print(digital_twin_property)

    # get all DigitalTwins info
    all_digital_twin = adt_client.get_all_digital_twin()
    print(all_digital_twin)

    # create and upsert new DigitalTwin
    digital_twin_id = 'Room3'
    digital_twin_model = 'dtmi:example:Room;1'
    digital_twin_property = {
        'Temperature': 60
    }  # property keys must match the DT-Model
    new_digital_twin = adt_client.upsert_digital_twin(digital_twin_id=digital_twin_id,
                                                      digital_twin_model=digital_twin_model,
                                                      digital_twin_property=digital_twin_property)
    print(new_digital_twin)

    # update existing DigitalTwin property
    patch_add = {'Humidity': 30}
    patch_replace = {'Humidity': 40}
    patch_remove = ['Temperature']
    adt_client.update_digital_twin(digital_twin_id=digital_twin_id,
                                   patch_add=patch_add,
                                   patch_replace=patch_replace,
                                   patch_remove=patch_remove)
    update_digital_twin = adt_client.get_digital_twin(digital_twin_id=digital_twin_id)
    print(update_digital_twin)

    # delete existing DigitalTwins
    digital_twin_ids = ['Room3', 'Room4', 'Room5', 'Room6']
    adt_client.delete_digital_twins(digital_twin_ids)

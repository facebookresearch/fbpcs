# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from unittest import TestCase
from data_transformation_lambda import lambda_handler
import base64
import json

class TestDataIngestion(TestCase):
    def setUp(self):
        self.sample_context = {} # Not used by the lambda for now

        self.sample_record_data = {'serverSideEvent': {
                    'event_time': 1234,
                    'custom_data': {'currency': 'usd', 'value': 2},
                    'event_name': 'Purchase',
                    'user_data': {
                        'em': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111',
                        'madid': 'bbbbbbbbbbbbbbbb2222222222222222'
                    },
                    'action_source': 'website'
                },
                'pixelId': '4321'
        }

    def test_non_encoded_data_is_transformed(self):
        event = self.sample_event(self.sample_record_data)
        result = lambda_handler(event, self.sample_context)
        self.assertEqual(result['records'][0]['recordId'], event["records"][0]['recordId'])
        self.assertEqual(result['records'][0]['result'], 'Ok')

    def test_encoded_data_is_transformed(self):
        event = self.sample_event(self.sample_record_data)
        result = lambda_handler(event, self.sample_context)
        encoded_data = result['records'][0]['data']
        decoded_data = base64.b64decode(encoded_data)
        decoded_dict = json.loads(decoded_data)
        server_side_event = self.sample_record_data['serverSideEvent']

        self.assertEqual(decoded_dict['data_source_id'], self.sample_record_data['pixelId'])
        self.assertEqual(decoded_dict['timestamp'], server_side_event['event_time'])
        self.assertEqual(decoded_dict['currency_type'], server_side_event['custom_data']['currency'])
        self.assertEqual(decoded_dict['conversion_value'], server_side_event['custom_data']['value'])
        self.assertEqual(decoded_dict['event_type'], server_side_event['event_name'])
        self.assertEqual(decoded_dict['email'], server_side_event['user_data']['em'])
        self.assertEqual(decoded_dict['device_id'], server_side_event['user_data']['madid'])
        self.assertEqual(decoded_dict['action_source'], server_side_event['action_source'])

    def test_server_side_event_error(self):
        malformed_dict = {"a" : "b"}
        event = self.sample_event(malformed_dict)
        result = lambda_handler(event, self.sample_context)

        # Assert the malformed row gets skipped!
        self.assertEqual(len(result['records']), 0)

    def test_null_row_skipped(self):
        null_dict = {'serverSideEvent': {
                'custom_data': {},
                'user_data': {},
                'action_source': 'website'
            },
            'pixelId': '4321'
        }
        event = self.sample_event(null_dict)
        result = lambda_handler(event, self.sample_context)

        self.assertEqual(len(result['records']), 0)

    def test_user_agent_parsed_fields(self):
        record = self.sample_record_data
        server_side_event = record['serverSideEvent']
        server_side_event['custom_data']['custom_properties'] = {
            'ignored': '1',
            '_cloudbridge_browser_name': 'Chrome Desktop',
            '_cloudbridge_device_os': 'Mac OS X',
            '_cloudbridge_device_os_version': '10.13.6',
        }
        event = self.sample_event(record)
        result = lambda_handler(event, self.sample_context)
        encoded_data = result['records'][0]['data']
        decoded_data = base64.b64decode(encoded_data)
        decoded_dict = json.loads(decoded_data)

        self.assertEqual(
            server_side_event['custom_data']['custom_properties']['_cloudbridge_browser_name'],
            decoded_dict['browser_name']
        )
        self.assertEqual(
            server_side_event['custom_data']['custom_properties']['_cloudbridge_device_os'],
            decoded_dict['device_os']
        )
        self.assertEqual(
            server_side_event['custom_data']['custom_properties']['_cloudbridge_device_os_version'],
            decoded_dict['device_os_version']
        )

    def sample_event(self, event):
        sample_encoded_data = base64.b64encode(json.dumps(event).encode('utf-8'))
        return {
            "invocationId": "invocationIdExample",
            "deliveryStreamArn": "arn:aws:kinesis:EXAMPLE",
            "region": "us-east-1",
            "records": [
                {
                    "recordId": "49546986683135544286507457936321625675700192471156785154",
                    "approximateArrivalTimestamp": 1495072949453,
                    "data": sample_encoded_data
                }
            ]
        }

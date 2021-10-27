# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from __future__ import print_function

import base64
import json
import os

# initiate
print("Loading lambda function...")


def lambda_handler(event, context):
    output = []
    ##### NOTE: this script assume the schema is correct, no missing items
    for record in event["records"]:

        row = {}
        recordId = record["recordId"]
        row["recordId"] = recordId
        row["result"] = "Ok"
        decoded_data = json.loads(base64.b64decode(record["data"]))

        dic = dict(os.environ.items())
        debug = "DEBUG" in dic.keys() and dic["DEBUG"] == "true"

        if debug:
            print(
                f"Processing record for recordId: {recordId}"
            )

        # if loaded as str, load again
        if type(decoded_data) is str:
            decoded_data = json.loads(decoded_data)

        if "serverSideEvent" not in decoded_data.keys():
            msg = f"Error: serverSideEvent does not exist for recordId: {recordId}"
            print(msg)
            continue
        row_data = decoded_data["serverSideEvent"]
        data_source_id = decoded_data.get("pixelId")
        # as of H2 2021, it should only be "website".
        action_source = row_data.get("action_source")
        timestamp = row_data.get("event_time")
        event_type = row_data.get("event_name")
        dummy_dict = {}
        currency_type = row_data.get("custom_data", dummy_dict).get("currency")
        conversion_value = row_data.get("custom_data", dummy_dict).get("value")
        email = row_data.get("user_data", dummy_dict).get("em")
        device_id = row_data.get("user_data", dummy_dict).get("madid")
        phone = row_data.get("user_data", dummy_dict).get("ph")
        client_ip_address = row_data.get("user_data", dummy_dict).get("client_ip_address")
        client_user_agent = row_data.get("user_data", dummy_dict).get("client_user_agent")
        click_id = row_data.get("user_data", dummy_dict).get("fbc")
        login_id = row_data.get("user_data", dummy_dict).get("fbp")
        custom_properties = row_data.get("custom_data", dummy_dict).get("custom_properties", dummy_dict)
        browser_name = custom_properties.get("_cloudbridge_browser_name")
        device_os = custom_properties.get("_cloudbridge_device_os")
        device_os_version = custom_properties.get("_cloudbridge_device_os_version")

        # make sure not all values are None
        if all(
            value is None
            for value in [
                timestamp,
                currency_type,
                conversion_value,
                event_type,
                email,
                device_id,
                phone,
                click_id,
                login_id,
            ]
        ):
            msg = f"All essential columns are None/Null. Skip recordId: f{recordId}"
            print(msg)
            continue

        data = {}
        user_data = {}
        data["data_source_id"] = data_source_id
        data["timestamp"] = timestamp
        data["currency_type"] = currency_type
        data["conversion_value"] = conversion_value
        data["event_type"] = event_type
        data["action_source"] = action_source
        if email:
            user_data["email"] = email
        if device_id:
            user_data["device_id"] = device_id
        if phone:
            user_data["phone"] = phone
        if client_ip_address:
            user_data["client_ip_address"] = client_ip_address
        if client_user_agent:
            user_data["client_user_agent"] = client_user_agent
        if click_id:
            user_data["click_id"] = click_id
        if login_id:
            user_data["login_id"] = login_id
        if browser_name:
            user_data["browser_name"] = browser_name
        if device_os:
            user_data["device_os"] = device_os
        if device_os_version:
            user_data["device_os_version"] = device_os_version

        data['user_data'] = user_data
        # firehose need data to be b64-encoded
        data = json.dumps(data) + "\n"
        data = data.encode("utf-8")
        row["data"] = base64.b64encode(data)
        output.append(row)

    print("finished data transformation.")
    return {"records": output}

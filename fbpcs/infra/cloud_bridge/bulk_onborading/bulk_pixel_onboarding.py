# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import csv
import json
import sys
import traceback

import requests
from requests import Response


def get_access_token(hostname: str, client_id: str, client_secret: str) -> str:

    url = f"https://{hostname}/clients/token"

    payload = f"client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(url, headers=headers, data=payload, verify=True)
    if response.status_code != 200:
        print(
            f"Not able to fetch the access token for client id: {client_id}, error: {response}"
        )
        quit()

    data = json.loads(response.content)

    return data["access_token"]


def add_pixel(
    hostname: str,
    tenant_id: str,
    pixel_id: str,
    business_id: str,
    access_token: str,
    pixel_access_token: str,
) -> Response:
    url = f"https://{hostname}/capig/graphql/"

    payload = (
        '{"query":"    mutation AddNewPixelModalMutation(\\n      $tenantId: ID!\\n      $input: '
        "PixelConnectionCreationInput!\\n    ) {\\n      tenantMutations(tenantId: $tenantId) {\\n  "
        "signalMutations {\\n          setupPixelSignalConfig(input: $input) {\\n            id\\n  "
        "domains\\n            connectionId\\n            connectionStatus {\\n              id\\n  "
        "accessTokenAvailable\\n              active\\n              eventBridgeActive\\n "
        "publishingEnabled\\n              apiErrorCode\\n              pixelID\\n              pixelName\\n  "
        "lastPublished\\n              totalEventsPublished\\n              lastReceived\\n             "
        'totalEventsReceived\\n            }\\n          }\\n        }\\n      }\\n    }","variables":{ '
        '"tenantId":"' + tenant_id + '","input":{"pixelId":"' + pixel_id + '", '
        '"businessId":"'
        + business_id
        + '","accessToken":"'
        + pixel_access_token
        + '","apiVersion":"v17.0", '
        '"externalId":""}}}'
    )

    headers = {
        "Authorization": "Bearer " + access_token + "",
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, data=payload, verify=True)

    return response


def read_access_tokens():
    access_token_dict = {}
    with open("business_access_tokens.csv", "r") as data:
        for line in csv.DictReader(data):
            access_token_dict[line["business_id"]] = line["access_token"]

    return access_token_dict


def main() -> None:
    # total arguments
    n = len(sys.argv)
    if n != 5:
        print("Missing argument. Required: 5, Total arguments passed:", n)
        return

    hostname = sys.argv[1]
    client_id = sys.argv[2]
    client_secret = sys.argv[3]
    tenant_id = sys.argv[4]

    access_token = get_access_token(hostname, client_id, client_secret)

    business_access_token_dict = read_access_tokens()

    pixel_file = "pixels.csv"
    print(f"Reading pixels from {pixel_file}.")
    with open(pixel_file, mode="r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            try:
                business_id = row["business_id"]
                pixel_id = row["pixel_id"]

                pixel_access_token = business_access_token_dict[business_id]
                response = add_pixel(
                    hostname,
                    tenant_id,
                    pixel_id,
                    business_id,
                    access_token,
                    pixel_access_token,
                )
                if response.status_code != 200:
                    print(f"Error adding pixelId: {pixel_id}, error: {response.text}")
                    return

                print(f"Added pixelId: {pixel_id}")

            except Exception:
                traceback.print_exc()


if __name__ == "__main__":
    main()

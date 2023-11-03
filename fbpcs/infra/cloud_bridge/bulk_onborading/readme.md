# Bulk Tenant Creation

This script is intend to onboard multiple pixels for given account/tenant.

## Pre Req:
**Genearting Client Id and Secret:**
1. Open `https://<capig.instance.url>/hub/settings/clients/`
2. Generate Client Id and Client secret.
3. Replace the values in the `business_access_tokens.csv`. For Access Token Generation can refer: https://developers.facebook.com/docs/marketing-api/conversions-api/get-started#access-token
3. Replace the values in the `pixels.csv`.


## Execution:
1. Install python3
2. Followed by `pip3 install requests`
3. Command: `python3 bulk_pixel_onboarding.py <hostname> <client_id> <client_secret> <tenant_id>`
4. Example: `python3 bulk_pixel_onboarding.py capig.cbinternal.com testclient 876418ce-ace4-4b72-8aee-5530a2b83ed0 04ThV94U`

import requests
import os
import uuid
import base64
from contextlib import contextmanager


@contextmanager
def load_env_from_file(filepath):
    """Load environment variables from file and clean up on exit."""
    env_vars = {}
    try:
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    env_vars[key] = os.getenv(key)
                    os.environ[key] = value
        yield
    finally:
        for key, original_value in env_vars.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value

def to_base64url(input_str: str) -> str:
    raw = input_str.encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw)
    return encoded.decode("utf-8").rstrip("=")

def main():
    with load_env_from_file("env.txt"):
        SOURCE_URL = os.getenv(
            "MX_THINK_SOURCE_URL", "https://source.example.com/api/submodels"
        )
        DEST_URL = os.getenv(
            "MX_THINK_DEST_URL", "https://dest.example.com/api/submodels"
        )

        TOKEN_URL = os.getenv(
            "MX_THINK_TOKEN_URL",
            "https://auth.example.com/realms/myrealm/protocol/openid-connect/token",
        )
        CLIENT_ID = os.getenv("MX_THINK_CLIENT_ID", "my-client-id")
        CLIENT_SECRET = os.getenv("MX_THINK_CLIENT_SECRET", "my-secret")

        SM_URN_PREFIX = "urn%3Aag.em%3Asm%3A"
        SM_URN_SUFFIX = ["%3Apcf%3A1.0.0", "%3Ahs%3A1.0.0", "%3Anameplate%3A1.0.0"]

        ASSETS = ["train.1", "measuring_wagon", "locomotive.cargo", "cargo_wagon.1"]

        SUBPROTOCOL_BODY_ID = os.getenv("SUBPROTOCOL_BODY_ID", "123")
        SUBPROTOCOL_BODY_DSP_ENDPOINT = os.getenv("SUBPROTOCOL_BODY_DSP_ENDPOINT", "http://edc.control.plane/api/v1/dsp")
        SUBPROTOCOL_BODY = f"id={SUBPROTOCOL_BODY_ID};dspEndpoint={SUBPROTOCOL_BODY_DSP_ENDPOINT}"
        DTR_SM_DESCR_URL = os.getenv("MX_THINK_DTR_SHELL_DESCR_URL", "foo")
        DATA_PLANE_URL = os.getenv("DATA_PLANE_URL", "123")

        TIMEOUT_SECONDS = 20
        DRY_RUN = os.getenv("DRY_RUN", "False").lower() == "true"

        def get_oauth_token():
            """Request OAuth2 client-credentials access token."""
            data = {
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }
            resp = requests.post(
                TOKEN_URL, data=data, verify=False, timeout=TIMEOUT_SECONDS
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Token request failed ({resp.status_code}): {resp.text}"
                )
            token = resp.json().get("access_token")
            if not token:
                raise RuntimeError("Token response did not contain 'access_token'.")
            return token

    def create_shell_descriptor(asset, submodel_descriptors):
        """
        Create an AAS shell-descriptor dict with the requested shape.

        Args:
            global_asset_id: Your global asset ID (IRI/URN/string).
            asset: Name of the Asset for id_short
            submodel_descriptors: A list of submodel descriptor dicts that will be attached as-is.

        Returns:
            A dictionary matching your shell descriptor format.
        """
        if not asset or not isinstance(asset, str):
            raise ValueError("asset must be a non-empty string.")
        
        if not isinstance(submodel_descriptors, list) or not all(isinstance(d, dict) for d in submodel_descriptors):
            raise ValueError("submodel_descriptors must be a list of dicts.")
        
        shell_id = f"urn:uuid:{uuid.uuid4()}"

        return {
            "id": shell_id,
            "idShort": asset,
            "globalAssetId": asset,
            "submodelDescriptors": submodel_descriptors,
        }

    def post_sm_descriptor(json, token):

        headers = {
            "Authorization": f"Bearer {token}" if token else "",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if DRY_RUN:
            print(f"Not posting submodel description to {DTR_SM_DESCR_URL} due to dry run.")
            return

        print(f"POSTing to {DTR_SM_DESCR_URL} ")
        post_resp = requests.post(
            DTR_SM_DESCR_URL,
            json=json,
            headers=headers,
            verify=False,
            timeout=TIMEOUT_SECONDS,
        )

        if post_resp.status_code in (200, 201):
            print(f"[OK] Posted item.")
        elif post_resp.status_code == 409:
            print(f"[SKIP] Already exists (409):")
        else:
            print(
                f"[FAIL]: HTTP {post_resp.status_code} {post_resp.text}"
            )
 
    def create_submodel_descriptor(submodel_ids):
        if not submodel_ids or not isinstance(submodel_ids, list):
                raise ValueError("submodel_id must be a non-empty list")
        
        submodel_descriptors = []

        for submodel_id in submodel_ids:
            base64url_submodel_id = to_base64url(submodel_id)
            submodel_endpoint = f"{DATA_PLANE_URL}/{base64url_submodel_id}"

            submodel_descriptors.append({
                "id": submodel_id,
                "semanticId": None,
                "endpoints": [
                    {
                    "interface": "SUBMODEL-3.0",
                    "protocolInformation": {
                        "href": submodel_endpoint,
                        "endpointProtocol": "HTTP",
                        "endpointProtocolVersion": [
                        "1.1"
                        ],
                        "subprotocol": "DSP",
                        "subprotocolBody": SUBPROTOCOL_BODY,
                        "subprotocolBodyEncoding": "plain",
                        "securityAttributes": [
                        {
                            "type": "NONE",
                            "key": "NONE",
                            "value": "NONE"
                        }
                        ]
                    }
                    }
                ]
            }
            )
            return submodel_descriptors
        
        def fetch_and_post_submodels(token):
            """Fetch each submodel, then post its dataSourceItems."""
            headers = {
                "Authorization": f"Bearer {token}" if token else "",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            posted_submodels = []

            # LOOP 1: through each asset
            for asset in ASSETS:
                print(f"\n{'='*60}")
                print(f"Processing Asset: {asset}")
                print(f"{'='*60}")

                # Create list of sm_ids for this asset
                sm_ids = [f"{SM_URN_PREFIX}{asset}{suffix}" for suffix in SM_URN_SUFFIX]

                # LOOP 2: through each submodel for this asset
                for sm_id in sm_ids:
                    url = f"{SOURCE_URL}/{sm_id}"

                    resp = requests.get(
                        url,
                        headers={"Accept": "application/json"},
                        verify=False,
                        timeout=TIMEOUT_SECONDS,
                    )
                    
                    if resp.status_code != 200:
                        print(
                            f"[WARN] Failed to fetch {sm_id}: {resp.status_code} {resp.text}"
                        )
                        yield ("failed", sm_id)
                        continue

                    submodel = resp.json()
                    
                    if not isinstance(submodel, dict):
                        yield ("failed", sm_id)
                        continue

                    label = submodel.get("id")

                    if DRY_RUN:
                        print(f"[DRY-RUN] Would POST item: {label}")
                        yield ("dry_run", label)
                        continue

                    post_resp = requests.post(
                        DEST_URL,
                        json=submodel,
                        headers=headers,
                        verify=False,
                        timeout=TIMEOUT_SECONDS,
                    )

                    if post_resp.status_code in (200, 201):
                        print(f"[OK] Posted item: {label}")
                        posted_submodels.append(label)
                        yield ("posted", label)
                    elif post_resp.status_code == 409:
                        print(f"[SKIP] Already exists (409): {label}")
                        posted_submodels.append(label)
                        yield ("skipped", label)
                    else:
                        print(
                            f"[FAIL] {label}: HTTP {post_resp.status_code} {post_resp.text}"
                        )
                        yield ("failed", label)
            
            submodel_descriptors = create_submodel_descriptor(posted_submodels)
            shell_descriptors = create_shell_descriptor(asset,submodel_descriptors)
            
            yield ("posted_submodels", posted_submodels)

        print("Requesting OAuth2 token...")
        if DRY_RUN:
            print("[DRY-RUN] Skipping token request.")
            token = None
        else:
            token = get_oauth_token()
            print("Token acquired.")

        print("Fetching and posting submodels and their data source items...")
        count = 0
        posted = 0
        skipped_409 = 0
        failed = 0

        for result_type, label in fetch_and_post_submodels(token):

            count += 1
            if result_type == "posted":
                posted += 1
            elif result_type == "skipped":
                skipped_409 += 1
            elif result_type == "failed":
                failed += 1

        print("\n--- Summary ---")
        print(f"Total source items: {count}")
        print(f"Posted:            {posted}")
        print(f"Skipped (409):     {skipped_409}")
        print(f"Failed:            {failed}")


if __name__ == "__main__":
    main()

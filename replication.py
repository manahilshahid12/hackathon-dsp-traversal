#!/usr/bin/env python3
import requests
import os
from contextlib import contextmanager

# ---------------------------------------------------------
# Configuration from environment variables
# ---------------------------------------------------------

@contextmanager
def load_env_from_file(filepath):
    """Load environment variables from file and clean up on exit."""
    env_vars = {}
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    env_vars[key] = os.getenv(key)
                    os.environ[key] = value
        yield
    finally:
        for key, original_value in env_vars.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value

def main():
    with load_env_from_file("env.txt"):
        SOURCE_URL = os.getenv("MX_THINK_SOURCE_URL", "https://source.example.com/api/submodels")
        DEST_URL   = os.getenv("MX_THINK_DEST_URL", "https://dest.example.com/api/submodels")

        TOKEN_URL       = os.getenv("MX_THINK_TOKEN_URL", "https://auth.example.com/realms/myrealm/protocol/openid-connect/token")
        CLIENT_ID       = os.getenv("MX_THINK_CLIENT_ID", "my-client-id")
        CLIENT_SECRET   = os.getenv("MX_THINK_CLIENT_SECRET", "my-secret")

        SM_URN_PREFIX = "urn%3Aag.em%3Asm%3A"
        SM_URN_SUFFIX = "%3Apcf%3A1.0.0"

        ASSETS = [
            "train.1",
            "measuring_wagon",
            "locomotive.cargo",
            "cargo_wagon.1"
        ]

        TIMEOUT_SECONDS = 20
        DRY_RUN = True
        # ---------------------------------------------------------


        def get_oauth_token():
            """Request OAuth2 client-credentials access token."""
            data = {
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }

            resp = requests.post(
                TOKEN_URL,
                data=data,
                verify=False,
                timeout=TIMEOUT_SECONDS
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Token request failed ({resp.status_code}): {resp.text}")

            token = resp.json().get("access_token")
            if not token:
                raise RuntimeError("Token response did not contain 'access_token'.")
            return token


        def fetch_and_post_submodels(token):
            """Generator that fetches each submodel and posts it, yielding results."""
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }

            submodel_ids = [f"{SM_URN_PREFIX}{asset}{SM_URN_SUFFIX}" for asset in ASSETS]
            
            for sm_id in submodel_ids:
                url = f"{SOURCE_URL}/{sm_id}"
                resp = requests.get(
                    url,
                    headers={"Accept": "application/json"},
                    verify=False,
                    timeout=TIMEOUT_SECONDS
                )
                if resp.status_code != 200:
                    print(f"[WARN] Failed to fetch {sm_id}: {resp.status_code} {resp.text}")
                    continue

                submodel = resp.json()
                if not isinstance(submodel, dict):
                    continue
                
                label = submodel.get("id", "<unknown>")

                if DRY_RUN:
                    print(f"[DRY-RUN] Would POST submodel: {label}")
                    yield ("dry_run", label)
                    continue

                post_resp = requests.post(
                    DEST_URL,
                    json=submodel,
                    headers=headers,
                    verify=False,
                    timeout=TIMEOUT_SECONDS
                )

                if post_resp.status_code in (200, 201):
                    print(f"[OK] Posted submodel: {label}")
                    yield ("posted", label)
                elif post_resp.status_code == 409:
                    print(f"[SKIP] Already exists (409): {label}")
                    yield ("skipped", label)
                else:
                    print(f"[FAIL] {label}: HTTP {post_resp.status_code} {post_resp.text}")
                    yield ("failed", label)


        print("Requesting OAuth2 token...")
        if DRY_RUN:
            print("[DRY-RUN] Skipping token request.")
            token=None
        else:
            token = get_oauth_token()
            print("Token acquired.")
        
        print("Fetching and posting submodels...")
        count = 0
        posted = 0
        skipped_409 = 0

        for result_type, label in fetch_and_post_submodels(token):
            count += 1
            if result_type == "posted":
                posted += 1
            elif result_type == "skipped":
                skipped_409 += 1

        print("\n--- Summary ---")
        print(f"Total source items: {count}")
        print(f"Posted:            {posted}")
        print(f"Skipped (409):     {skipped_409}")


if __name__ == "__main__":
    main()
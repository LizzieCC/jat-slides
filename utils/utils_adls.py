# io_adls.py 
from __future__ import annotations

import os
from typing import Dict, Optional, Literal, Tuple
from datetime import datetime, timedelta, timezone

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions

from contextlib import contextmanager

# ---  ---
storage_account_name: str = os.getenv("AZURE_STORAGE_ACCOUNT", "cfcetlsadls")
_credential = None  # cache DefaultAzureCredential

# ---------------- Helpers ----------------
def parse_storage_from_path(path) -> Tuple[Literal["datalake", "local"], Optional[str]]:
    """
    Returns (storage_mode, container) based on the path.

    Valid patterns:
      - /vsiaz/<container>/...
      - az://<container>/...
      - az//:<container>/...   (lenient: accepts this variant)
      - (anything else) -> local

    Rules:
      - If vsiaz/az scheme -> container is REQUIRED and mode='datalake'
      - Else -> container=None and mode='local'
    """
    path = os.fspath(path)  # handles Path, str
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")

    prefixes = ("/vsiaz/", "az://")
    matched_prefix = next((p for p in prefixes if path.startswith(p)), None)

    if matched_prefix is None:
        return "local", None

    rest = path[len(matched_prefix):]
    first = rest.split("/", 1)[0] if rest else ""
    if not first:
        raise ValueError(f"Container is required after '{matched_prefix}'.")
    return "datalake", first

def get_credential(**kwargs):
    """
    Create (once) and reuse DefaultAzureCredential.
    kwargs forwarded to DefaultAzureCredential (e.g. exclude_* flags).
    """
    global _credential
    if _credential is not None:
        return _credential
    if DefaultAzureCredential is None:
        raise RuntimeError(
            "azure-identity is required for ADLS access. Install it and/or add it to dependencies."
        )
    # sensible default for CI/headless; override by passing kwargs
    if "exclude_shared_token_cache_credential" not in kwargs:
        kwargs["exclude_shared_token_cache_credential"] = True
    _credential = DefaultAzureCredential(**kwargs)
    return _credential

def storage_options(path: str) -> Dict:
    """
    Dictionary for pandas/pyarrow/geopandas storage_options param.
    If storage_mode == "datalake", returns account_name + DefaultAzureCredential().
    Otherwise returns {} so local paths work unchanged.
    """
    storage_mode, container = parse_storage_from_path(path)
    if storage_mode == "datalake":
        return {
            "account_name": storage_account_name,
            "credential": get_credential(),
        }
    return {}

def create_container_sas(
    container: str,
    *,
    allow: Literal["read", "read_list", "read_write","read_write_del"] = "read_write_del",
    account_name: Optional[str] = None,
    ttl_hours: int = 2,
    **credential_kwargs,
) -> str:
    """
    Return a container-level User Delegation SAS (string WITHOUT leading '?').

    Requires the caller (DefaultAzureCredential principal) to have
    'Storage Blob Delegator' on the storage account.

    allow:
      - "read"       -> r
      - "read_list"  -> r,l
      - "read_write" -> r,l,w,c
    """
    acct = account_name or storage_account_name
    cred = get_credential(**credential_kwargs)
    account_url = f"https://{acct}.blob.core.windows.net"
    bsc = BlobServiceClient(account_url=account_url, credential=cred)

    # Small negative skew so token is immediately valid
    start = datetime.now(timezone.utc) - timedelta(minutes=5)
    expiry = start + timedelta(hours=ttl_hours)

    udk = bsc.get_user_delegation_key(start, expiry)

    if allow == "read":
        perms = ContainerSasPermissions(read=True)
    elif allow == "read_list":
        perms = ContainerSasPermissions(read=True, list=True)
    elif allow == "read_write":
        perms = ContainerSasPermissions(read=True, list=True, write=True, create=True)
    elif allow == "read_write_del":
        perms = ContainerSasPermissions(read=True, list=True, write=True, create=True, delete=True)
    else:
        raise ValueError(f"Unknown allow='{allow}'")

    sas = generate_container_sas(
        account_name=acct,
        container_name=container,
        user_delegation_key=udk,
        permission=perms,
        start=start,
        expiry=expiry,
    )
    return sas

def rasterio_env_kwargs(
    account_mode: str,
    *,
    account_name: Optional[str] = None,
    sas_token: Optional[str] = None,
    debug: bool = False,
    gdal_fast: bool = True,
) -> Dict[str, str]:
    """
    Returns dict of GDAL/Rasterio env vars.
    - local -> {}
    - datalake -> requires sas_token (no leading '?')
    """
    if account_mode != "datalake":
        return {}
    acct = account_name or storage_account_name
    if not acct:
        raise ValueError("account_name is required (arg or AZURE_STORAGE_ACCOUNT).")
    if not sas_token:
        raise ValueError("sas_token is required in datalake mode.")
    env = {
        "AZURE_STORAGE_ACCOUNT": acct,
        "AZURE_STORAGE_SAS_TOKEN": sas_token,
    }
    if gdal_fast:
        env["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
    if debug:
        env["CPL_DEBUG"] = "ON"
        env["CPL_CURL_VERBOSE"] = "TRUE"
    return env

def rasterio_env(
    account_mode: str,
    *,
    container: Optional[str] = None,
    account_name: Optional[str] = None,
    sas_token: Optional[str] = None,
    auto_sas: bool = True,
    allow: Literal["read", "read_list", "read_write"] = "read_write",
    ttl_hours: int = 4,
    debug: bool = False,
    gdal_fast: bool = True,
    **credential_kwargs,
):
    """
    Context manager for Rasterio.Env; imported lazily so the module works without Rasterio.
    - local mode -> plain Env()
    - datalake   -> uses provided SAS or auto-mints a container SAS
    """
    from contextlib import contextmanager
    @contextmanager
    def _ctx():
        # Lazy import here — only needed if you actually use Rasterio
        from rasterio import Env

        if account_mode != "datalake":
            with Env():
                yield
            return

        acct = account_name or storage_account_name
        if sas_token is None and auto_sas:
            if not container:
                raise ValueError("container is required to auto-mint a SAS.")
            sas = create_container_sas(
                container=container,
                account_name=acct,
                allow=allow,
                ttl_hours=ttl_hours,
                **credential_kwargs,
            )
        else:
            sas = sas_token

        env_kwargs = rasterio_env_kwargs(
            "datalake",
            account_name=acct,
            sas_token=sas,
            debug=debug,
            gdal_fast=gdal_fast,
        )

        with Env(**env_kwargs):
            yield
    return _ctx()

# --------------------------------
def _gdal_env_vars_for_azure(
    account_name: str,
    sas_token: str,
    *,
    debug: bool = False,
    gdal_fast: bool = True,
    gdal_random_write: bool = True,
) -> Dict[str, str]:
    env = {
        "AZURE_STORAGE_ACCOUNT": account_name,
        "AZURE_STORAGE_SAS_TOKEN": sas_token,   # no leading '?'
    }
    if gdal_fast:
        env["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
    if debug:
        env["CPL_DEBUG"] = "ON"
        env["CPL_CURL_VERBOSE"] = "TRUE"
    if gdal_random_write:
        env["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES"
    return env

@contextmanager
def gdal_azure_session(
    *,
    path: str,  # REQUIRED: we infer mode+container from this
    account_name: Optional[str] = None,
    sas_token: Optional[str] = None,
    auto_sas: bool = True,
    allow: Literal["read", "read_list", "read_write","read_write_del"] = "read_write_del",
    ttl_hours: int = 4,
    debug: bool = False,
    gdal_fast: bool = True,
    gdal_random_write: bool = False,
    **credential_kwargs,
):
    """
    A single context that makes **both** Rasterio and GeoPandas/Fiona/pyogrio
    see Azure credentials, then cleans up.
    """
    from rasterio import Env  # lazy import

    storage_mode, container = parse_storage_from_path(path)

    if storage_mode == "local":
        with Env():
            yield
        return
    
    # datalake branch
    if not container:
        raise ValueError("Container is required for datalake paths.")

    acct = account_name or storage_account_name
    if not acct:
        raise ValueError("account_name is required (arg or AZURE_STORAGE_ACCOUNT).")

    # Acquire SAS
    if sas_token is None and auto_sas:
        if not container:
            raise ValueError("container is required to auto-mint a SAS.")
        # You plug in your own issuer here
        sas = create_container_sas(
            container=container,
            account_name=acct,
            allow=allow,
            ttl_hours=ttl_hours,
            **credential_kwargs,
        )
    else:
        sas = sas_token
    if not sas:
        raise ValueError("sas_token is required in datalake mode.")

    # Build env vars for all GDAL consumers
    new_env = _gdal_env_vars_for_azure(
        account_name=acct,
        sas_token=sas,
        debug=debug,
        gdal_fast=gdal_fast,
        gdal_random_write=gdal_random_write
    )

    # Save/restore old env safely
    old_env = {k: os.environ.get(k) for k in new_env}
    try:
        os.environ.update(new_env)                  # make visible to Fiona/pyogrio
        with Env(**{k: v for k, v in new_env.items() if k.startswith("GDAL_") or k.startswith("CPL_")}):
            yield
    finally:
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

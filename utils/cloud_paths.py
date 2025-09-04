from upath import UPath as Path
import fsspec
from cfc_core_utils import storage_options

def cloud_exists(p: Path) -> bool:
    if getattr(p, "protocol", "file") == "file":
        return p.exists()
    fs = fsspec.filesystem("az", **storage_options(p))
    return fs.exists(str(p))

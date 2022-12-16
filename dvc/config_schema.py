import os
from urllib.parse import urlparse

from funcy import walk_values
from voluptuous import (
    All,
    Any,
    Coerce,
    Invalid,
    Lower,
    Optional,
    Range,
    Schema,
)

Bool = All(
    Lower,
    Any("true", "false"),
    lambda v: v == "true",
    msg="expected true or false",
)


def supported_cache_type(types):
    """Checks if link type config option consists only of valid values.

    Args:
        types (list/string): type(s) of links that dvc should try out.
    """
    if types is None:
        return None
    if isinstance(types, str):
        types = [typ.strip() for typ in types.split(",")]

    unsupported = set(types) - {"reflink", "hardlink", "symlink", "copy"}
    if unsupported:
        raise Invalid(
            "Unsupported cache type(s): {}".format(", ".join(unsupported))
        )

    return types


def Choices(*choices):
    """Checks that value belongs to the specified set of values

    Args:
        *choices: pass allowed values as arguments, or pass a list or
            tuple as a single argument
    """
    return Any(*choices, msg="expected one of {}".format(", ".join(choices)))


def ByUrl(mapping):
    schemas = walk_values(Schema, mapping)

    def validate(data):
        if "url" not in data:
            raise Invalid("expected 'url'")

        parsed = urlparse(data["url"])
        # Windows absolute paths should really have scheme == "" (local)
        if os.name == "nt" and len(parsed.scheme) == 1 and parsed.netloc == "":
            return schemas[""](data)
        if parsed.scheme not in schemas:
            raise Invalid(f"Unsupported URL type {parsed.scheme}://")

        return schemas[parsed.scheme](data)

    return validate


class RelPath(str):
    pass


REMOTE_COMMON = {
    "url": str,
    "checksum_jobs": All(Coerce(int), Range(1)),
    "jobs": All(Coerce(int), Range(1)),
    Optional("worktree"): Bool,
    Optional("no_traverse"): Bool,  # obsoleted
    Optional("version_aware"): Bool,
}
LOCAL_COMMON = {
    "type": supported_cache_type,
    Optional("protected", default=False): Bool,  # obsoleted
    "shared": All(Lower, Choices("group")),
    Optional("slow_link_warning", default=True): Bool,
    Optional("verify", default=False): Bool,
}
HTTP_COMMON = {
    "auth": All(Lower, Choices("basic", "digest", "custom")),
    "custom_auth_header": str,
    "user": str,
    "password": str,
    "ask_password": Bool,
    "ssl_verify": Any(Bool, str),
    "method": str,
    Optional("verify", default=False): Bool,
}
WEBDAV_COMMON = {
    "user": str,
    "password": str,
    "ask_password": Bool,
    "token": str,
    "cert_path": str,
    "key_path": str,
    "timeout": Coerce(int),
    "ssl_verify": Any(Bool, str),
    Optional("verify", default=False): Bool,
}

SCHEMA = {
    "core": {
        "remote": Lower,
        "checksum_jobs": All(Coerce(int), Range(1)),
        Optional("interactive", default=False): Bool,
        Optional("analytics", default=True): Bool,
        Optional("hardlink_lock", default=False): Bool,
        Optional("no_scm", default=False): Bool,
        Optional("autostage", default=False): Bool,
        Optional("experiments"): Bool,  # obsoleted
        Optional("check_update", default=True): Bool,
        "machine": Lower,
    },
    "cache": {
        "local": str,
        "s3": str,
        "gs": str,
        "hdfs": str,
        "webhdfs": str,
        "ssh": str,
        "azure": str,
        # This is for default local cache
        "dir": str,
        **LOCAL_COMMON,
    },
    "remote": {
        str: ByUrl(
            {
                "": {**LOCAL_COMMON, **REMOTE_COMMON},
                "s3": {
                    "region": str,
                    "profile": str,
                    "credentialpath": str,
                    "configpath": str,
                    "endpointurl": str,
                    "access_key_id": str,
                    "secret_access_key": str,
                    "session_token": str,
                    Optional("listobjects", default=False): Bool,  # obsoleted
                    Optional("use_ssl", default=True): Bool,
                    "ssl_verify": Any(Bool, str),
                    "sse": str,
                    "sse_kms_key_id": str,
                    "sse_customer_algorithm": str,
                    "sse_customer_key": str,
                    "acl": str,
                    "grant_read": str,
                    "grant_read_acp": str,
                    "grant_write_acp": str,
                    "grant_full_control": str,
                    "cache_regions": bool,
                    "read_timeout": Coerce(int),
                    "connect_timeout": Coerce(int),
                    Optional("verify", default=False): Bool,
                    **REMOTE_COMMON,
                },
                "gs": {
                    "projectname": str,
                    "credentialpath": str,
                    "endpointurl": str,
                    Optional("verify", default=False): Bool,
                    **REMOTE_COMMON,
                },
                "ssh": {
                    "type": supported_cache_type,
                    "port": Coerce(int),
                    "user": str,
                    "password": str,
                    "ask_password": Bool,
                    "passphrase": str,
                    "ask_passphrase": Bool,
                    "keyfile": str,
                    "timeout": Coerce(int),
                    "gss_auth": Bool,
                    "allow_agent": Bool,
                    Optional("verify", default=False): Bool,
                    **REMOTE_COMMON,
                },
                "hdfs": {"user": str, "kerb_ticket": str, **REMOTE_COMMON},
                "webhdfs": {
                    "kerberos": Bool,
                    "kerberos_principal": str,
                    "proxy_to": str,
                    "ssl_verify": Any(Bool, str),
                    "token": str,
                    "use_https": Bool,
                    Optional("verify", default=False): Bool,
                    **REMOTE_COMMON,
                },
                "azure": {
                    "connection_string": str,
                    "sas_token": str,
                    "account_name": str,
                    "account_key": str,
                    "tenant_id": str,
                    "client_id": str,
                    "client_secret": str,
                    "allow_anonymous_login": Bool,
                    "exclude_environment_credential": Bool,
                    "exclude_visual_studio_code_credential": Bool,
                    "exclude_shared_token_cache_credential": Bool,
                    "exclude_managed_identity_credential": Bool,
                    Optional("verify", default=False): Bool,
                    **REMOTE_COMMON,
                },
                "oss": {
                    "oss_key_id": str,
                    "oss_key_secret": str,
                    "oss_endpoint": str,
                    Optional("verify", default=True): Bool,
                    **REMOTE_COMMON,
                },
                "gdrive": {
                    "profile": str,
                    "gdrive_use_service_account": Bool,
                    "gdrive_client_id": str,
                    "gdrive_client_secret": str,
                    "gdrive_user_credentials_file": str,
                    "gdrive_service_account_user_email": str,
                    "gdrive_service_account_json_file_path": str,
                    Optional("gdrive_trash_only", default=False): Bool,
                    Optional("gdrive_acknowledge_abuse", default=False): Bool,
                    Optional("verify", default=True): Bool,
                    **REMOTE_COMMON,
                },
                "http": {**HTTP_COMMON, **REMOTE_COMMON},
                "https": {**HTTP_COMMON, **REMOTE_COMMON},
                "webdav": {**WEBDAV_COMMON, **REMOTE_COMMON},
                "webdavs": {**WEBDAV_COMMON, **REMOTE_COMMON},
                "remote": {str: object},  # Any of the above options are valid
            }
        )
    },
    "state": {
        "dir": str,
        "row_limit": All(Coerce(int), Range(1)),  # obsoleted
        "row_cleanup_quota": All(Coerce(int), Range(0, 100)),  # obsoleted
    },
    "index": {
        "dir": str,
    },
    "machine": {
        str: {
            "cloud": All(Lower, Choices("aws", "azure")),
            "region": All(
                Lower, Choices("us-west", "us-east", "eu-west", "eu-north")
            ),
            "image": str,
            "spot": Bool,
            "spot_price": Coerce(float),
            "instance_hdd_size": Coerce(int),
            "instance_type": Lower,
            "instance_gpu": Lower,
            "ssh_private": str,
            "startup_script": str,
            "setup_script": str,
        },
    },
    # section for experimental features
    "feature": {
        Optional("machine", default=False): Bool,
        # enabled by default. It's of no use, kept for backward compatibility.
        Optional("parametrization", default=True): Bool,
    },
    "plots": {
        "html_template": str,
        Optional("auto_open", default=False): Bool,
        "out_dir": str,
    },
    "exp": {
        "code": str,
        "data": str,
        "models": str,
        "metrics": str,
        "params": str,
        "plots": str,
        "live": str,
    },
    "parsing": {
        "bool": All(Lower, Choices("store_true", "boolean_optional")),
        "list": All(Lower, Choices("nargs", "append")),
    },
    "hydra": {
        Optional("enabled", default=False): Bool,
        "config_dir": str,
        "config_name": str,
    },
}

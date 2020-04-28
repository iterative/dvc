from enum import Enum


class StageParams:
    PARAM_MD5 = "md5"
    PARAM_CMD = "cmd"
    PARAM_WDIR = "wdir"
    PARAM_DEPS = "deps"
    PARAM_OUTS = "outs"
    PARAM_LOCKED = "locked"
    PARAM_META = "meta"
    PARAM_ALWAYS_CHANGED = "always_changed"
    PARAM_PARAMS = "params"


class OutputParams(Enum):
    PERSIST = "outs_persist"
    PERSIST_NO_CACHE = "outs_persist_no_cache"
    METRICS_NO_CACHE = "metrics_no_cache"
    METRICS = "metrics"
    NO_CACHE = "outs_no_cache"
    OUTS = "outs"

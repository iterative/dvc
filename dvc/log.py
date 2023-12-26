# using a separate module instead of using `dvc.logger` to not create an import-cycle.
import logging


class LoggerWithTrace(logging.Logger):
    # only for type checking
    trace = logging.debug


logger: "LoggerWithTrace" = logging.getLogger()  # type: ignore[assignment]

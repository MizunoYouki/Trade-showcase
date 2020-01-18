import logging
import sys

from io import StringIO
from typing import TextIO, Union


def get_logger(name: str,
               stream: Union[str, TextIO] = StringIO(),
               _format: str = '%(levelname)s:%(message)s',
               level='info',
               datefmt: str = None):
    logger = logging.getLogger(name)

    if not stream:
        logger.disabled = True
        return logger

    handler = None
    if isinstance(stream, str):
        if stream.lower() == 'stdout':
            handler = logging.StreamHandler()
        if stream.lower() == 'stderr':
            handler = logging.StreamHandler(sys.stderr)
    else:
        handler = logging.StreamHandler(stream)

    if level.lower() == 'debug':
        _level = logging.DEBUG
    elif level.lower() == 'info':
        _level = logging.INFO
    elif level.lower() == 'warning':
        _level = logging.WARNING
    elif level.lower() == 'error':
        _level = logging.ERROR
    elif level.lower() == 'critical':
        _level = logging.CRITICAL
    else:
        raise Exception('Unsupported level: {}'.format(level))

    logger.setLevel(_level)

    if _format:
        fmt = _format
    else:
        fmt = None

    if datefmt:
        _datefmt = datefmt
    else:
        _datefmt = None

    if not logger.handlers:
        formatter = logging.Formatter(fmt, _datefmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


class LoggingAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f'[{self.extra["class"]}.{self.extra["method"]}] {msg}', kwargs

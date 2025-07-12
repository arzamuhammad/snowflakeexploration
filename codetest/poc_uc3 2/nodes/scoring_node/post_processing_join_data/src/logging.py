import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

LOG_FILENAME = "logs-{}.log"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_MAXBYTES = "500000000"
LOG_MAXBACKUP = "5"


"""
    Provides services for the logging functionality in the entire application
      with additional information from
    middleware functions.

    ...

    Functions
    -----------
    setupLogging: logger
        Used for returning the logger object for the provided instance name.

"""


def setupLogging(logger_name):
    """
    Sets up the logger function with the given logger name and handlers
    and returns the logger object.

    ...

    Attributes
    ------------
    logger_name: object
        Includes the instance object name for which the logger needs to be set up.
    """

    _logger = logging.getLogger(logger_name)
    _formatter = logging.Formatter(LOG_FORMAT)

    _log_filename = LOG_FILENAME.format(datetime.now().strftime("%Y%m%d%H%M%S"))
    _logFileHandler = RotatingFileHandler(
        filename=_log_filename,
        mode="a",
        maxBytes=int(LOG_MAXBYTES),
        backupCount=int(LOG_MAXBACKUP),
    )
    _logFileHandler.setFormatter(_formatter)

    _consoleHandler = logging.StreamHandler((sys.stdout))
    _consoleHandler.setFormatter(_formatter)

    _logger.addHandler(_logFileHandler)
    _logger.addHandler(_consoleHandler)
    _logger.setLevel(logging.INFO)

    return _logger

import sys
import logging


def configure_logging(logger, level, log_file=None):
    # This should not be called twice, log an error and return
    if len(logger.handlers):
        logger.error('LOGGING ALREADY SET UP BUT configure_logging CALLED AGAIN')
        return

    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)-6s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
    )

    if log_file:
        # Log log_level and above to a file, if specified
        log_level_handler = logging.FileHandler(log_file)
        log_level_handler.setFormatter(formatter)
        logger.addHandler(log_level_handler)

    # In addition, log warning and above to stderr
    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setLevel(logging.WARN)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

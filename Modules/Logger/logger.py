import logging


def get_logger(name):
    from Modules.Logger import appender

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if len(logger.handlers) == 0:
        logger.addHandler(appender.file())
    logger.propagate = False
    return logger

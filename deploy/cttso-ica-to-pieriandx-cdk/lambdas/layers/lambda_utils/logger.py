#!/usr/bin/env python3

import logging

from .globals import LOGGER_STYLE

# Set logger
logger = logging.getLogger()
# Set basic logger
logger.setLevel(level=logging.INFO)

# Set formatter
formatter = logging.Formatter(LOGGER_STYLE)

# Set console handler
console_hander = logging.StreamHandler()
console_hander.setLevel(logging.INFO)
console_hander.setFormatter(formatter)

# Add console handler to logger
logger.addHandler(console_hander)


def get_logger():
    return logging.getLogger()

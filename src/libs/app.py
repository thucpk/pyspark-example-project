import json
import logging


def start_app(app_name, config_file=None):
    config_dict = {
        "job_name": app_name
    }
    logger = logging.getLogger(__name__)
    if config_file:
        config_dict['params'] = json.load(config_file)

    return logger, config_dict

import os
import json
import logging.config


def setup_logging(
        module_name,
        default_path=os.getcwd() + '/config/logging.json',
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        config['handlers']['info_file_handler']['filename'] = config['handlers']['info_file_handler'][
            'filename'].format(
            module_name=module_name)
        config['handlers']['error_file_handler']['filename'] = config['handlers']['error_file_handler'][
            'filename'].format(
            module_name=module_name)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

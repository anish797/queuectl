import json
import os

config_file = 'queuectl_config.json'

default_config = {
    'max_retries': 5,
    'backoff_base': 3,
    'job_timeout': 300,
}

def load_config():
    if not os.path.exists(config_file):
        return default_config.copy()
    
    try:
        with open(config_file, 'r') as f:
            loaded_config = json.load(f)
            return {**default_config, **loaded_config}
    except (json.JSONDecodeError, IOError):
        return default_config.copy()

def save_config(config):
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)

def get(key):
    config = load_config()
    return config.get(key, default_config.get(key))

def set_value(key, value):
    config = load_config()
    config[key] = value
    save_config(config)

def get_all():
    return load_config()
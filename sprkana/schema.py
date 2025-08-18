
import logging as log
_schema = None

def set_schema(new_schema):
    global _schema
    _schema = new_schema

def schema_safe(func):
    """
    Decorator to ensure the schema is set before running the function.
    If the schema is not set, it raises an error.
    """
    def wrapper(*args, **kwargs):
        if _schema is None:
            raise ValueError("Schema is not set. Please set the schema before running this function.")
        return func(*args, **kwargs)
    return wrapper

@schema_safe
def get_schema():
    return _schema

@schema_safe
def add_to_schema(path, d=None, sep="/"):
    log.info(f'Adding variable \'{path}\' to schema...')
    keys = path.split(sep)
    if d is None:
        d = get_schema()
    current = d
    for key in keys[:-1]:  # Traverse until the second-to-last key
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}  # Create a new sub-dictionary if missing
        current = current[key]
    # Add the final key as a value (here we just set value = key, as in your example)
    current[keys[-1]] = keys[-1]
    set_schema(d)
    pass
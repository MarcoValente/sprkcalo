
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

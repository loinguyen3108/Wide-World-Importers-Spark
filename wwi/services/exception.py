class SchemaNotMatch(Exception):
    def __init__(self, error_message=None) -> None:
        super().__init__(error_message=error_message)
    
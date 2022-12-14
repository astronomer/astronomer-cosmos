class BaseParser:
    """
    A base Parser class that all parsers should inherit from.
    """

    def __init__(self):
        pass

    def parse(self):
        """
        Parses the given data into a `cosmos` entity.

        :return: The parsed entity
        :rtype: Task or Group
        """
        raise NotImplementedError()

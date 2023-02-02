class OSMException(Exception):
    """Exception when Open Signal Matrix does not exist."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: some context why error occurred while
        using Open Signal Matrix
        """
        super(OSMException, self).__init__(reason)


class GenomeException(Exception):
    """Exception when OSM does not exist."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: some context why genome is not available
        """
        super(GenomeException, self).__init__(reason)


class QualityException(Exception):
    """Exception, when quality of the bed file didn't pass."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: reason why quality control wasn't successful
        """
        super(QualityException, self).__init__(reason)


class RequirementsException(Exception):
    """Exception, when quality of the bed file didn't pass."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: reason why quality control wasn't successful
        """
        super(RequirementsException, self).__init__(reason)

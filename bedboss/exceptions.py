class OSMException(Exception):
    """Exception when OSM does not exist."""

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

        :param str reason: some context why genome is not avaliable
        """
        super(Genomexception, self).__init__(reason)

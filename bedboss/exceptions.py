class BedBossException(Exception):
    """Exception, when bedboss fails."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: some context why error occurred while
        using BedBoss
        """
        super(BedBossException, self).__init__(reason)


class OpenSignalMatrixException(BedBossException):
    """Exception when Open Signal Matrix does not exist."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: some context why error occurred while
        using Open Signal Matrix
        """
        super(OpenSignalMatrixException, self).__init__(reason)


class QualityException(BedBossException):
    """Exception, when quality test of the bed file didn't pass."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: reason why quality control wasn't successful
        """
        self.reason = reason
        super(QualityException, self).__init__(reason)


class RequirementsException(BedBossException):
    """Exception, when requirement packages are not installed."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: additional info about requirements exception
        """
        super(RequirementsException, self).__init__(reason)


class BedTypeException(BedBossException):
    """Exception when Bed Type could not be determined."""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: some context why error occurred while
        using Open Signal Matrix
        """
        super(BedTypeException, self).__init__(reason)


class ValidatorException(BedBossException):
    """Exception when there is an exception during refgenome validation"""

    def __init__(self, reason: str = ""):
        """
        Optionally provide explanation for exceptional condition.

        :param str reason: some context why error occurred
        """
        super(BedTypeException, self).__init__(reason)

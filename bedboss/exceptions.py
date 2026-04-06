class BedBossException(Exception):
    """Exception, when bedboss fails."""

    def __init__(self, reason: str = "") -> None:
        """
        Optionally provide explanation for exceptional condition.

        Args:
            reason: Some context why error occurred while using BedBoss.
        """
        super().__init__(reason)


class OpenSignalMatrixException(BedBossException):
    """Exception when Open Signal Matrix does not exist."""

    def __init__(self, reason: str = "") -> None:
        """
        Optionally provide explanation for exceptional condition.

        Args:
            reason: Some context why error occurred while using Open Signal Matrix.
        """
        super().__init__(reason)


class QualityException(BedBossException):
    """Exception, when quality test of the bed file didn't pass."""

    def __init__(self, reason: str = "", file_size: int = 0) -> None:
        """
        Optionally provide explanation for exceptional condition.

        Args:
            reason: Reason why quality control wasn't successful.
            file_size: File size in bytes (if available).
        """
        self.reason = reason
        self.file_size = file_size
        super().__init__(reason)


class RequirementsException(BedBossException):
    """Exception, when requirement packages are not installed."""

    def __init__(self, reason: str = "") -> None:
        """
        Optionally provide explanation for exceptional condition.

        Args:
            reason: Additional info about requirements exception.
        """
        super().__init__(reason)


class BedTypeException(BedBossException):
    """Exception when Bed Type could not be determined."""

    def __init__(self, reason: str = "") -> None:
        """
        Optionally provide explanation for exceptional condition.

        Args:
            reason: Some context why the bed type could not be determined.
        """
        super().__init__(reason)


class ValidatorException(BedBossException):
    """Exception when there is an exception during refgenome validation."""

    def __init__(self, reason: str = "") -> None:
        """
        Optionally provide explanation for exceptional condition.

        Args:
            reason: Some context why error occurred.
        """
        super().__init__(reason)

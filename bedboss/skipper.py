# This module will serve to skip samples that were already processed.
import os


class Skipper:
    def __init__(self, output_path: str, name: str):
        self.output_path = output_path
        self.name = name

        self.file_path = os.path.join(output_path, f"{name}.log")
        self.file_fail_log_path = os.path.join(output_path, f"{name}_fail.log")

        self.info = self._read_log_file(self.file_path)

    def is_processed(self, sample_name: str) -> str | bool:
        """
        Check if a sample was already processed.

        Args:
            sample_name: Name of the sample.

        Returns:
            Digest of the sample, or False if not processed.
        """
        return self.info.get(sample_name, False)

    def add_processed(
        self, sample_name: str, digest: str, success: bool = False
    ) -> None:
        """
        Add a sample to the processed list.

        Args:
            sample_name: Name of the sample.
            digest: Digest of the sample.
            success: If the processing was successful.
        """
        # line = f"{sample_name}\t{digest}\t{'success' if success else 'failed'}\n"
        line = f"{sample_name},{digest}\n"
        with open(self.file_path, "a") as file:
            file.write(line)

        self.info[sample_name] = digest

    def _create_log_file(self, file_path: str) -> None:
        """
        Create a log file.

        Args:
            file_path: Path to the log file.
        """
        with open(file_path, "w") as file:
            file.write("")

    def _read_log_file(self, file_path: str) -> dict:
        """
        Read the log file.

        Args:
            file_path: Path to the log file.

        Returns:
            Dictionary mapping sample names to digests.
        """
        data_dict = {}
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                for line in file:
                    # Split each line by whitespace
                    columns = line.strip().split(",")

                    # Assign the first column as the key, and the rest as the value
                    if len(columns) >= 2:  # Ensure there are at least three columns
                        key = columns[0]
                        value = columns[1]
                        data_dict[key] = value
        else:
            self._create_log_file(file_path)

        return data_dict

    def create_fail_log(self):
        """Create a log file for failed samples."""
        if not os.path.exists(self.file_fail_log_path):
            self._create_log_file(self.file_fail_log_path)

    def reinitialize(self):
        """Reinitialize the log file."""
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
            self.info = self._read_log_file(self.file_path)
        else:
            self.info = self._read_log_file(self.file_path)

    def add_failed(self, sample_name: str, error: str = None):
        """
        Add a sample to the failed list.

        Args:
            sample_name: Name of the sample.
            error: Error message.
        """
        line = f"{sample_name},{error}\n"
        with open(self.file_fail_log_path, "a") as file:
            file.write(line)

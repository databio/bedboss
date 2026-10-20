from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from bedboss.bbuploader import main
from bedboss.bbuploader.models import ProjectProcessingStatus


@pytest.fixture
def fake_upload(monkeypatch):
    """Run upload_all over fake GSEs; each GSE's _upload_gse result comes from `outcomes`."""
    outcomes = []
    peps = SimpleNamespace(count=0, results=[])

    def fake_upload_gse(gse, **kwargs):
        outcome = outcomes[int(gse)]
        if isinstance(outcome, Exception):
            raise outcome
        return ProjectProcessingStatus(
            number_of_samples=outcome, number_of_processed=outcome
        )

    session = MagicMock()
    session.__enter__.return_value = session
    session.scalar.return_value = None

    monkeypatch.setattr(main, "PEPHubClient", MagicMock())
    monkeypatch.setattr(main, "BedBaseAgent", MagicMock())
    monkeypatch.setattr(main, "Session", MagicMock(return_value=session))
    monkeypatch.setattr(main.pypiper, "PipelineManager", MagicMock())
    monkeypatch.setattr(main, "standardize_genome_name", lambda g, **kw: g)
    monkeypatch.setattr(main, "find_peps", lambda **kw: peps)
    monkeypatch.setattr(main, "_upload_gse", fake_upload_gse)

    def run(*results):
        outcomes[:] = results
        peps.results = [SimpleNamespace(name=str(i)) for i in range(len(results))]
        peps.count = len(results)
        main.upload_all(bedbase_config="x", outfolder="/tmp", lite=True)

    return run


def test_upload_all_stops_on_database_error(fake_upload):
    # The 2026-08 outage: every new file died on the bed insert, yet the job exited 0.
    err = IntegrityError("INSERT INTO bed ...", {}, Exception("not-null"))
    with pytest.raises(IntegrityError):
        fake_upload(3, err)


def test_upload_all_continues_past_bad_input(fake_upload):
    # Bad files (gtars parse errors etc.) stay logged-and-skipped.
    fake_upload(RuntimeError("stream did not contain valid UTF-8"), 3)

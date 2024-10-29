import peppy
import pytest

from bedboss.utils import standardize_pep


# @pytest.mark.skip(reason="Not for automatic testing")
@pytest.mark.parametrize(
    "registry_path",
    [
        "bedbase/gse274894:samples",
        "bedbase/gse275349:samples",
        "bedbase/gse262920:samples",
        "bedbase/gse236101:samples",
        "bedbase/gse254365:samples",
    ],
)
@pytest.mark.parametrize(
    "model",
    ["BEDBASE", "ENCODE"],
)
def test_standardize_pep(registry_path, model):
    pep = peppy.Project.from_pephub(registry_path)
    standardize_pep(pep, model=model)
    assert True

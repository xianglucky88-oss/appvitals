import pytest

def test_versionline_is_open_gap():
    from src.appvitals import versionline
    with pytest.raises(NotImplementedError):
        versionline.process({"id": "x"})

def test_review_page_is_open_gap():
    from src.appvitals import review_page
    with pytest.raises(NotImplementedError):
        review_page.process({"id": "x"})

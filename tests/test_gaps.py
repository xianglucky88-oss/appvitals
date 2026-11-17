import pytest

def test_review_page_is_open_gap():
    from src.appvitals import review_page
    with pytest.raises(NotImplementedError):
        review_page.process({"id": "x"})

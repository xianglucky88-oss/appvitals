import pytest

def test_versionline_is_open_gap():
    from src.appvitals import versionline
    with pytest.raises(NotImplementedError):
        versionline.process({"id": "x"})

def test_review_page_is_implemented_and_passthrough_safe():
    # review_page 已实现：非 envelope 记录原样透传，envelope 走分页采集契约
    from src.appvitals import review_page
    legacy = {"id": "x"}
    assert review_page.process(dict(legacy)) == legacy
    envelope = {"kind": "review_page", "app_id": "a", "store": "s",
                "page_number": 1, "has_more": False, "reviews": []}
    review_page.reset_state()
    out = review_page.process(envelope)
    assert isinstance(out, list)
    assert out[0]["kind"] == "review_page_checkpoint"
    review_page.reset_state()

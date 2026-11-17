import unittest

from src.appvitals import versionline, review_page


class OpenGapTests(unittest.TestCase):
    def test_versionline_is_open_gap(self):
        with self.assertRaises(NotImplementedError):
            versionline.process({"id": "x"})

    def test_review_page_is_open_gap(self):
        with self.assertRaises(NotImplementedError):
            review_page.process({"id": "x"})


if __name__ == "__main__":
    unittest.main()

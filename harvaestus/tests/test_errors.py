from unittest import TestCase

from harvaestus.errors import FixableError


class TestFixableError(TestCase):

    def test_key(self):
        error = FixableError("mykey")
        self.assertEqual(error.error_key, "mykey")

    def test_data(self):
        error = FixableError("mykey", request=1, some_other_key=2)
        self.assertEqual({"request": 1, "some_other_key": 2}, error.data)

    def test_raisable(self):
        error = FixableError("mykey", request=1, some_other_key=2)

        with self.assertRaises(FixableError):
            raise error

    def test_equality(self):
        self.assertEqual(FixableError("mykey"), FixableError("mykey"))
        self.assertNotEqual(FixableError("mykey"), FixableError("mykey2"))


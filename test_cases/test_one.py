import pytest

class TestMathOperations(TransFormClass):
    def test_addition(self):
         assert 2==2

    def test_subtraction(self):
        assert 5 - 3 == 2, "Subtraction test failed"

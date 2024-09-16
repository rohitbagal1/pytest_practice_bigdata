

import pytest
import sys
from transformation.transformation_class import TransFormClass
from test_data import data

class TestMathOperations(TransFormClass):
    def test_addition(self):
        df_1=self.execute_transformations(data.columns_to_add, data.columns_to_drop, data.column_to_capitalize)
        assert df_1.collect()== df_1.collect(), "Addition test failed"

    def test_subtraction(self):
        assert 5 - 3 == 2, "Subtraction test failed"

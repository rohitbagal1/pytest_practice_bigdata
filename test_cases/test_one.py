import pytest
from test_data import data
from transformation.transformation_class import TransFormClass
class TestMathOperations(TransFormClass):
    def test_addition(self):
        
         df=self.execute_transformations(data.columns_to_add, data.columns_to_drop, data.column_to_capitalize)
         assert df.collect()!=df.collect(),"having hava error"

    def test_subtraction(self):
        assert 5 - 3 == 2, "Subtraction test failed"

import pytest
#from test_data import data
from transformation.transformation_class import TransFormClass
class TestMathOperations(TransFormClass):
    def test_addition(self):
         columns_to_add= ['city']
         columns_to_drop = ['city']
         column_to_capitalize = ['name']
         df=self.execute_transformations(columns_to_add, columns_to_drop, column_to_capitalize)
         assert df.collect()!=df.collect(),"having hava error"

    def test_subtraction(self):
        assert 5 - 3 == 2, "Subtraction test failed"

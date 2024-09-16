from base_class.dev_class import BaseDev
from test_data import data

class TransFormClass(BaseDev):
    def read_file_from_path(self):
        data1 = [(1, "Alice","Purnia","12/06/2003","23/09/2050",-1),
                 (2, "Bob","Jaipur","12/06/2033","23/09/2055",0),
                 (3, "Charlie","Delhi","12/06/2053","23/09/2055",1)]    
        columns1 = ["id", "name","Address","dob","dob1","status"]
        df = spark.createDataFrame(data1, columns1)
        return df
        

    def add_time_stamp_to_df(self, df):
        df_with_time_stamp = self.add_start_end_time_columns(df)
        return df_with_time_stamp

    def adding_column_city(self, df, columns_to_add_in_df):
        df_with_city_column = self.add_columns(df, columns_to_add_in_df)
        return df_with_city_column

    def dropping_column_city(self, df, columns_to_drop):
        df_without_city_column = self.drop_columns_from_df(df, columns_to_drop)
        return df_without_city_column

    def captilize_name_column_content(self, column, dataframe):
        df_with_name_column_capital = self.col_to_upper(column, dataframe)
        return df_with_name_column_capital

    def execute_transformations(self, columns_to_add, columns_to_drop, column_to_capitalize):
        df = self.read_file_from_path()
        # Add timestamp to the DataFrame
        df_with_time_stamp = self.add_time_stamp_to_df(df)
        
        # Add city column to the DataFrame
        df_with_city_column = self.adding_column_city(df_with_time_stamp, columns_to_add)
        
        # Drop city column from the DataFrame
        df_without_city_column = self.dropping_column_city(df_with_city_column, columns_to_drop)
        
        # Capitalize name column content
        final_df = self.captilize_name_column_content(column_to_capitalize, df_without_city_column)
        
        return final_df




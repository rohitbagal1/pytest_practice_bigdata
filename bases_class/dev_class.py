import logging
from pyspark.sql.functions import col , trim
from pyspark.sql.functions import col, initcap
from pyspark.sql.functions import split, when
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, concat, expr, substring, lit, upper
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
import logging
import os
from pyspark.sql.functions import trim, col, regexp_replace
from delta import DeltaTable
from pyspark.sql import *
from functools import reduce
import copy
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from matplotlib import pyplot as plt
import seaborn as sns
from functools import reduce
from functools import reduce
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id, col, when
from base_class.test_class import BaseTest


class BaseDev(BaseTest): 
    def __init__(self):
        self.status_l=[]
        self.table_name_list=[]
        self.result_dict = {}
        #logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(message)s')
    
############################ Paridhi's Methods###################################################################

  
        
    def fix_primary_key_column_by_dropping_duplicates(self,df, unique_keys):
        f=0
        # Remove duplicates based on unique keys
        try:
            self.logger.info(f"Removing duplicates and updating them with unique combinations in Pk list of : {unique_keys} ")
            deduplicated_df = df.dropDuplicates(unique_keys)
            # Generate unique values for unique key columns
            unique_df = deduplicated_df.withColumn('surrogate_key', monotonically_increasing_id())
            #Remove this after review
            res=self.check_duplicates_for_primary_keys(unique_df,unique_keys)
            self.logger.info("")
            # Show the resulting DataFrame with unique primary key values
            return unique_df
        except Exception as e:
            self.logger.error(f"Error replacing duplicate values with unique values in PK list of {unique_keys}: {e}")
            self.logger.info("")
            return f

        
    
    # NULLS HOTO CHLEGA BUT SBME EK SATH NAHI
    def enforce_non_null_primary_keys(self,df, primary_key_columns):
        f=0
        try:
            self.logger.info(f"Removing nulls and updating them with unique values in Pk combination list of : {unique_keys}")
            # Check for null values in the unique key combination
            combined_null_check = col(primary_key_columns[0]).isNull()
            for col_name in primary_key_columns[1:]:
                combined_null_check = combined_null_check & col(col_name).isNull()

            # Replace null values with unique values only if all columns in the unique key combination are null
            unique_value_condition = combined_null_check
            for col_name in primary_key_columns:
                df = df.withColumn(col_name, when(unique_value_condition, monotonically_increasing_id()).otherwise(col(col_name)))
                # return df
            result=self.check_nulls_in_primary_keys(df, primary_key_columns)
            self.logger.info(f"Consecutive nulls found in PK list or single PK : {result}")
            self.logger.info("")
            return df
        except Exception as e:
            self.logger.error(f"Error while removing nulls and updating them with unique values in PK combination list: {e}")
            self.logger.info("")
            return f

    
    def drop_columns_from_df(self,df, columns_to_drop):
        try:
            self.logger.info(f"Dropping columns present in {columns_to_drop} list.")
            self.logger.info("")
            updated_df = df.drop(*columns_to_drop)
            return updated_df
        except Exception as e:
            self.logger.error(f"Error dropping columns {columns_to_drop}: {e}")
            self.logger.info("")
            return df
    
    def add_start_end_time_columns(self,df):
        f=0
        try:
            self.logger.info(f"Adding columns for start_timeStamp and end_timeStamp: ")
            self.logger.info("")
            # Add a column for start time with the current timestamp
            df_with_start_time = df.withColumn("start_timeStamp", current_timestamp())
    
            # Add a column for end time with the default value "9999-01-01"
            df_with_end_time = df_with_start_time.withColumn("end_timeStamp", lit("9999-01-01"))
            return df_with_end_time
        except Exception as e:
            self.logger.error(f"Error adding columns : {e}")
            self.logger.info("")
            return f
    
    def add_columns(self,df,columns_to_add_in_df):
        f=0
        try:
            self.logger.info(f"Adding columns as per the list in metadata file : {columns_to_add_in_df} ")
            self.logger.info("")
            for col_name in columns_to_add_in_df:
                df = df.withColumn(col_name, lit(None))  # You can initialize the columns with a default value
            return df
        except Exception as e:
            self.logger.error(f"Error adding columns : {e}")
            self.logger.info("")
            return f
        
    
    def trim_spaces_in_column(self, df, column_name_to_trim_spaces):
        f = 0
        try:
            self.logger.info(f"Removing extra spaces from column : {column_name_to_trim_spaces}")
            self.logger.info("")
            df = df.withColumn(column_name_to_trim_spaces, trim(col(column_name_to_trim_spaces)))
            df = df.withColumn(column_name_to_trim_spaces, regexp_replace(col(column_name_to_trim_spaces), r'\s+', ' '))
            return df
        except Exception as e:
            self.logger.error(f"Error removing spaces in column  : {e}")
            self.logger.info("")
            return f
    
    def count_nulls_in_columns(self,df):
        try:
            self.logger.info(f"Counting nulls present in each column of the dataframe.")
            self.logger.info("")
            null_counts = [df.where(col(c).isNull()).count() for c in df.columns]
            # Create a dictionary with column names and corresponding null counts
            null_counts_dict = dict(zip(df.columns, null_counts))
            return null_counts_dict
        except Exception as e:
            self.logger.error(f"Error counting nulls present in each column of the dataframe  : {e}")
            self.logger.info("")
            return f
    

    
    def dynamic_split_column(self,df, column_name, delimiter, part_num, new_column_name):
        f=0
        try:
            self.logger.info(f"Splitting column :{column_name} into its first part based on {delimiter}")
            self.logger.info("")
            return df.withColumn(new_column_name, expr(f"split_part({column_name}, '{delimiter}', {part_num})"))
        except Exception as e:
            self.logger.error(f"Error Splitting column :{column_name} into its first part based on {delimiter}:{e} ")
            self.logger.info("")
            return f
    
    def check_is_active_column(self,df, is_active_column):
        f=0
        try:
            self.logger.info(f"Checking {is_active_column} column to have values only 0 or 1.")
            self.logger.info("")
        # Check if the is_active column contains only '0' and '1' values
            result = df.withColumn("is_valid", when(col(is_active_column).startswith("0") | col(is_active_column).startswith("1"), "True").otherwise("False"))
            return result
        except Exception as e:
            self.logger.error(f"Error Checking {is_active_column} column to have values only 0 or 1 :{e} ")
            self.logger.info("")
            return f
    
    def transform_full_name(self,df, column_name):
        f=0
        try:
            self.logger.info(f"Separating first name from {column_name} .")
            self.logger.info("")
            df = df.withColumn(column_name_to_capitalize, col(column_name_to_capitalize).alias(column_name_to_capitalize.capitalize()))
            return df
    
        except Exception as e:
            self.logger.error(f"Error Separating first name from {column_name} :{e} .")
            self.logger.info("")
            return f
    
    def replace_null_with_string(self,df, column_names, replace_string):
        f=0
        try:
            self.logger.info(f"Replacing Nulls present in {column_names} column with NA for now.")
            self.logger.info("")
            """
            Replacing null with NA for now.
            """
            for column_name in column_names:
                df = df.withColumn(column_name, when(col(column_name).isNull(), replace_string).otherwise(col(column_name)))
                return df 
        except Exception as e:
            self.logger.error(f"Error Replacing Nulls present in {column_names} column with NA for now :{e}")
            self.logger.info("")
            return f
    
    def capitalize_initial_letters(self,df, column_names):
        f=0
        try:
            self.logger.info(f"Capitalizing first letter of {column_names}.")
            self.logger.info("")
            for column_name in column_names:
                df = df.withColumn(column_name, initcap(col(column_name)))
                return df   
        except Exception as e:
            self.logger.error(f"ErrorCapitalizing first letter of {column_names} :{e}")
            self.logger.info("")
            return f
    
    def mask_column(self,df, column_name):
        try:
            self.logger.info(f"Masking values present in  {column_name} column.")
            self.logger.info("")
            masked_column = F.concat(F.lit("******"), F.substring(F.expr(f"`{column_name}`"), -4, 4))
            return df.withColumn(column_name, masked_column)
        except Exception as e:
                self.logger.error(f"Error masking values present in {column_name} column :{e}")
                self.logger.info("")
                return f
    
    def calculate_months_between(self,df, start_date_column, end_date_column):
        try:
            self.logger.info(f"Calculating months between {start_date_column} col & {end_date_column} col.")
            self.logger.info("")
            return df.withColumn("months_between", months_between(df[end_date_column], df[start_date_column]))
        except Exception as e:
                self.logger.error(f"Error Calculating months between {start_date_column} col & {end_date_column} col :{e}")
                self.logger.info("")
                return f
                        

############################ SAURAV"S Methods ###################################################################


    def format_date(self,dataframe,columns_to_replace):
        column_names = dataframe.columns

        #print(type(columns_to_replace))
     
        for col_name in columns_to_replace:
            dataframe = dataframe.withColumn(col_name, regexp_replace(col(col_name), "/", "-"))
            
        return dataframe
    
    def schema_to_string(self,dataframe):
        try:
            if dataframe!=None:
                self.logger.info("Converting Schema to String.")
                simple_str=dataframe.schema.simpleString()
                return simple_str
            else:
                self.logger.error('DataFrame is empty')
        except Exception as e:
            self.logger.error(f"An error occurred while changing to string: {e}")


            
    
    def schema_to_json(self,dataframe):
        try:
            if dataframe!=None:
                self.logger.info("Converting Schema to JSON.")
                simple_json=dataframe.schema.json()
                return simple_json
            else:
                self.logger.error('DataFrame is empty')
        except Exception as e:
            self.logger.error(f"An error occurred while changing to JSON {e}")

    def df_to_pandas(self,dataframe):
        try:
            if dataframe!=None:
                self.logger.info("Converting Dataframe to Pandas.")
                df_pandas=dataframe.toPandas()
                return df_pandas
            else:
                self.logger.error('DataFrame is empty')
        except Exception as e:
            self.logger.error(f"An error occurred while changing to Pandas {e}")

    def col_to_upper(self,column,dataframe):
        try:
            if dataframe!=None:
                column_list=column
                df=dataframe
                for i in column_list:
                    self.logger.info(f"The rows of column called {i} is converted to Upper Case")
                    df=df.withColumn(i,upper(i))
                return df
            else:
                self.logger.error('DataFrame is empty')

        except Exception as e:
            self.logger.error(f"An error occurred while changing to uppercase {e}")
    
    def merge_dataframes(self,dataframe1,dataframe2,common_column,type_join):
        try:
            if (dataframe1 and dataframe2)!=None:
                self.logger.info("Merging the dataframes.")
                df1=dataframe1
                df2=dataframe2

                df3=df1.join(df2,df1.common_column==df2.common_column,type_join)
                return df3
            else:
                self.logger.error('DataFrame is empty')

        except Exception as e:
            self.logger.error(f"An error occurred while joining {e}")

    def merge_dataframes_no_duplicated_col(self,dataframe1,dataframe2,column_name,join_type):
        try:
            if (dataframe1 and dataframe2)!=None:
                self.logger.info("Merging dataframes with distict columns.")
                df1=dataframe1
                df2=dataframe2

                df3=df1.join(df2,[column_name],type_join)
                return df3
            else:
                self.logger.error('DataFrame is empty')

        except Exception as e:
            self.logger.error(f"An error occurred while joining {e}")

    def save_table_format(self,dataframe,format_table,table_name):
        try:
            if (dataframe and format_table and table_name)!=None:
                self.logger.info("Saving table")
                dataframe.write.format(format_table).saveAsTable(table_name)

            else:
                self.logger.error('DataFrame is empty')

        except Exception as e:
            self.logger.error(f"An error occurred while saving {e}")

    def fetch_all_dataframes(self):
        try:
            self.logger.info("Fetching all Dataframes")
            for key,val in globals().items():
                if type(val)==DataFrame:
                    print(key)

        except Exception as e:
            self.logger.error(f"An error occurred while fetching dataframe {e}")

    def add_missing_columns(self,dataframe1,dataframe2):
        try:
            if (dataframe1 and dataframe2)!=None:

                all_columns= dataframe1.columns + dataframe2.columns
                unique_columns=list(set(all_columns))
                self.logger.info("Adding Missing Columns")
                for col in unique_columns:
                    if col not in dataframe1.columns:
                        dataframe1=dataframe1.withColumn(col, lit(None))

                    if col not in dataframe2.columns:
                        dataframe2=dataframe2.withColumn(col, lit(None))

                return dataframe1,dataframe2
            else:
                self.logger.error('DataFrame is empty')
        
        except Exception as e:
            self.logger.error(f"An error occurred while adding missing columns {e}")

    

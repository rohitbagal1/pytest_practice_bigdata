from pyspark.sql.functions import col, trim, split, when, monotonically_increasing_id, concat, expr, substring, lit, upper, regexp_replace, concat_ws
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
from delta import DeltaTable
from functools import reduce
import copy
import uuid
from matplotlib import pyplot as plt
import seaborn as sns

class BaseTest:
    def __init__(self):
        self.status_l=[]
        self.table_name_list=[]
        self.result_dict = {}

    def add_to_status_list(self, flag):
        self.status_l.append(flag)
    
    def operation_result(self, a, b, c, d,e):
        caller_name = inspect.currentframe().f_back.f_code.co_name
        if caller_name=='empty_string_count_result' or caller_name=='null_count_result' or caller_name=='primary_col_validation':
            caller_name=caller_name+"_"+a+"_"+b
        else:
            caller_name=caller_name+"_"+a
        f= datetime.datetime.now()
        self.result_dict[caller_name] = {'table_name': a, 'column_name': b, 'Value': c,'Flag':d,'Status':e, 'Time_Stramp': f}
    
    def display_status_list(self):
        return self.status_l
    
    def display_result_dict(self):
        nested_dict=self.result_dict
        rows = [Row(**{'row_name': key, **value}) for key, value in nested_dict.items()]
        df = spark.createDataFrame(rows)
        return df
    
    #################################################################################

    # Creating df if file path is given.
    def create_df_from_csv_path(self,path):
        f=0
        try:
            self.logger.info(f"Created df using path {path}")
            self.logger.info("")
            # dataframe = spark.read.format(expected_format).load(path, header = "True")
            #df=spark.read.csv(path)
            df = spark.read.csv(path,header=True)
            #display(dataframe)
            return df
        except Exception as e:
            self.logger.error(f"Error loading DataFrame for {path}: {e}")
            self.logger.info("")
            return f

    
    # Creating df if table path is given. 
    def create_df_by_sql_query(self, table_name,database_name):
        f=0
        try:
            self.logger.info(f"Created df using table name {table_name} ")
            self.logger.info("")
            dataframe= spark.sql(f"select * from {database_name}.{table_name}")
            f=1
            return dataframe,f
        except Exception as e:
            self.logger.error(f"Error loading DataFrame for {database_name}.{table_name}: {e}")
            self.logger.info("")
            return f
    
    
    def count_rows(self, dataframe):
        f=0
        try:
            self.logger.info("Counting rows in df")
            count = dataframe.count()
            f=1
            return count
        except Exception as e:
            self.logger.error(f"Error occurred while counting rows: {e}")
            return f
    
    def check_empty_strings_and_count(self, dataframe):
        f=0
        if dataframe is not None:
            # column_list = dataframe.columns
            try:
                empty_string_counts = {}
                # Iterate over each column
                self.logger.info("Checking for empty strings and count of columns in df")
                for column in column_list:
                # Count the number of empty strings in the column
                    empty_string_count = dataframe.filter(col(column) == '').count()
                    empty_string_counts[column] = empty_string_count
                    f=1
                return empty_string_counts
            except Exception as e:
                self.logger.error(f"Error occurred while counting empty strings: {e}")
                return f

    
    def count_duplicates(self, df):
        f = 0
        try:
            self.logger.info(f"Counting duplicates in columns mentioned in {column_list}")
            if df is not None:
                # Count duplicate records
                duplicate_count = df.groupBy(*column_list).count().filter("count > 1").count()
                return duplicate_count
        except Exception as e:
            self.logger.error(f"Error occurred while counting duplicates: {e}")
            return f
    

    

    
    
    def count_unique_values(self , df):
        f=0
        if df is not None:
            try:
                self.logger.info(f"Counting unique values in df : ")
                total_unique_count = df.distinct().count()
                return total_unique_count
            except Exception as e:
                self.logger.error(f"Error occurred while counting unique values in df : {e}")
                return f
    

    def check_duplicates_for_primary_keys(self, df, unique_keys):
        #  Check for duplicates based on unique key columns
        duplicate_rows = df.groupBy(unique_keys).count().where('count > 1')
    
        if duplicate_rows.count() == 0:
            self.logger.info("No duplicate primary key combinations found after testing.")
            self.logger.info("")
            return duplicate_rows.count()
        
        else:
            self.logger.error("Duplicate primary key combinations found after testing:")
            self.logger.info("")
            return duplicate_rows.count()


    def check_nulls_in_primary_keys(self,df, primary_key_columns):
        # Check for null values in the combination of primary key columns
        combined_null_check = col(primary_key_columns[0]).isNull()
        for col_name in primary_key_columns[1:]:
            combined_null_check = combined_null_check & col(col_name).isNull()

        # Check if all primary key columns are not simultaneously null
        if df.filter(combined_null_check).count() > 0:
            self.logger.info("The DataFrame contains null values in the primary key columns simultaneously after testing.")
            return df.filter(combined_null_check).count()
        else:
            self.logger.error(f"The DataFrame does not contain null values in the primary key columns simultaneously after testing.")
            return df.filter(combined_null_check).count()

    def count_rows(self,dataframe):
        
        try:
            if dataframe!=None:
                self.logger.info("Fetching Dataframe...")
                return dataframe.count()
        except Exception as e:
            print(f"An error occurred while counting rows {e}")
            #self.logger.error()
        
    ############### SAURAV'S ###################################################################

    def create_table_name_list(self,file_path):
        try:
            list_of_all_tables = dbutils.fs.ls(file_path)
            # iterating list to find table name
            for table_name in list_of_all_tables:
            # Finding table name
                #table_name = table_name[1][:-1] for gold layer in smithfield
                table_name = table_name[1][:]
                self.table_name_list.append(table_name)
            return self.table_name_list
        except Exception as e:
            print(f"An error occurred while listing table names {e}")

    def create_column_list(self, dataframe):
        try:
            if dataframe!=None:
                self.logger.info("Fetching Dataframe...")
                return sorted(dataframe.columns)
        
            else:
                print('DataFrame is empty')
        except Exception as e:
            print(f"An error occurred while sorting columns {e}")

    def create_column_datatype_list(self,dataframe):
        try:
            if dataframe!=None:
                self.logger.info("Fetching Dataframe...")
                column_data_types = dataframe.dtypes
                return sorted(column_data_types)
            else:
                self.logger.error('DataFrame is empty')
               
        except Exception as e:
            self.logger.error(f"An error occurred while fetching datatypes {e}")
            #print(f"An error occurred while fetching datatypes {e}")
        
    def create_datatype_list(self,dataframe):
        try:
            if dataframe!=None:
                self.logger.info("Fetching Dataframe...")
                datatype_list=[]
                column_data_types = dataframe.dtypes
                for i in column_data_types:
                    datatype_list.append(i[1])

                return (datatype_list)
            else:
                print('DataFrame is empty')
        except Exception as e:
            self.logger.error(f"An error occurred while fetching datatypes {e}")
            #print(f"An error occurred while fetching datatypes {e}")

    def dropping_columns(self,dataframe,*column_list):
        try:
            if dataframe!=None:
                columns_list=list(column_list)
                dropped_df=dataframe.drop(*columns_list)

                return (dropped_df)
            else:
                self.logger.error('DataFrame is empty')
    
        except Exception as e:
            self.logger.error(f"An error occurred while dropping columns {e}")

    

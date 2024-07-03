import os
import pandas as pd
import warnings
import re

warnings.filterwarnings("ignore")

class ExcelDataHandler:
    """
    A class for handling Excel data and generating SQL statements.
    
    Args:
        schema_name (str): The name of the database schema.
        sheet_name (str, optional): The name of the Excel sheet to read. Defaults to "Raw Data".
    """
    def __init__(self, schema_name, sheet_name="Raw Data"):
        self.schema_name = schema_name
        self.sheet_name = sheet_name

    def search_and_merge_xlsx(self, folder_path):
        """
        Searches for all Excel files in the specified folder path and merges them into a single DataFrame.
        
        Args:
            folder_path (str): The path to the folder containing the Excel files.
        
        Returns:
            pandas.DataFrame: The merged DataFrame.
        """
        all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.xlsx')]
        df_list = [pd.read_excel(file, sheet_name=self.sheet_name) for file in all_files]
        
        merged_df = pd.concat(df_list, ignore_index=True)
        return merged_df

    def remove_duplicate_rows(self, df, column_name="Response ID"):
        """
        Removes duplicate rows from the DataFrame based on the specified column name.
        
        Args:
            df (pandas.DataFrame): The DataFrame to remove duplicate rows from.
            column_name (str): The name of the column to check for duplicates. Defaults to "Response ID".
        
        Returns:
            pandas.DataFrame: The DataFrame with duplicate rows removed.
        """
        try:
            return df.drop_duplicates(subset=[column_name])
        except KeyError:
            print(f"Column '{column_name}' not found in the DataFrame.")
            return df

    def sanitize_column_name(self, col_name):
        """
        Sanitizes the column name to make it SQL-compatible by removing or replacing special characters and truncating if necessary.
        
        Args:
            col_name (str): The original column name.
        
        Returns:
            str: The sanitized column name.
        """
        # Replace non-alphanumeric characters, except Arabic characters, with underscores
        sanitized = re.sub(r'[^\w\s\u0600-\u06FF]', '_', col_name)
        # Truncate to 63 characters, the maximum length for PostgreSQL identifiers
        return sanitized[:63]

    def ALTER_TABLE(self, table_name):
        """
        Generates the SQL ALTER TABLE statement to add an 'id' column and set it as the primary key.
        
        Args:
            table_name (str): The name of the table.
        
        Returns:
            str: The SQL ALTER TABLE statement.
        """
        alter_sentence = f"""
        ALTER TABLE "{self.schema_name}"."{table_name}" ADD id SERIAL;
        ALTER TABLE "{self.schema_name}"."{table_name}" ADD PRIMARY KEY (id);
        """
        return alter_sentence
    
    def sql_remove_duplicates(self, table_name, column_name):
        """
        Generates the SQL statement to remove duplicate rows from the specified table.
        
        Note: The table must have an 'id' column for this to work.
        
        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column to check for duplicates.
        
        Returns:
            str: The SQL statement to remove duplicate rows.
        """
        return f"""
        DELETE FROM "{self.schema_name}"."{table_name}"
        WHERE ID NOT IN
        (
            SELECT MAX(ID)
            FROM "{self.schema_name}"."{table_name}"
            GROUP BY "{column_name}"
        );"""

    def create_table_sql(self, table_name, df, sanitize_column_names=False):
        """
        Generates the SQL statement to create a table based on the DataFrame columns and data types.
        
        Args:
            table_name (str): The name of the table.
            df (pandas.DataFrame): The DataFrame to create the table from.
            sanitize_column_names (bool, optional): Whether to sanitize column names for SQL compatibility. Defaults to False.
        
        Returns:
            str: The SQL statement to create the table.
        """
        def map_dtype(dtype):
            if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
                return 'INTEGER'
            else:
                return 'TEXT'

        dtypes = df.dtypes
        sql_dtypes = dtypes.apply(map_dtype)

        if sanitize_column_names:
            columns_sql = ', '.join([f'"{self.sanitize_column_name(col)}" {dtype}' for col, dtype in zip(df.columns, sql_dtypes)])
        else:
            columns_sql = ', '.join([f'"column_{i+1}" {dtype}' for i, dtype in enumerate(sql_dtypes)])

        create_table_sql = f'CREATE TABLE "{self.schema_name}"."{table_name}" ({columns_sql});'
        create_table_sql = f'{create_table_sql}\n\n{self.ALTER_TABLE(table_name)}'


        # THIS WILL NOT WORK BECAUSE SOME COLUMNS ARE INTEGER THAT CANNOT BE INSERTED AS TEXT (FOR EXAMPLE, 'Response ID' COLUMN)
        # # Insert headers as a row
        # headers_values = ', '.join([f"'{col}'" for col in df.columns])
        # insert_headers_sql = f'INSERT INTO "{self.schema_name}"."{table_name}" ({columns_sql}) VALUES ({headers_values});'

        # return f'{create_table_sql}\n\n{insert_headers_sql}'
        return create_table_sql

    def format_value(self, value):
        """
        Formats the value for SQL insertion.
        
        Args:
            value: The value to format.
        
        Returns:
            str: The formatted value.
        """
        if pd.isna(value):
            return 'NULL'
        elif isinstance(value, str):
            value = value.replace("'", "\"")
            return f"'{value}'"
        elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
            return f"'{value}'"
        else:
            return str(value)
    
    def insert_data_sql(self, table_name, df, skip_duplicates=False, sanitize_column_names=False):
        """
        Generates an SQL INSERT statement for inserting data from a DataFrame into a specified table.

        Args:
            table_name (str): The name of the table where the data will be inserted.
            df (pandas.DataFrame): The DataFrame containing the data to be inserted.
            skip_duplicates (bool, optional): Whether to skip inserting duplicate rows. Defaults to False.

        Returns:
            str: The SQL INSERT statement.
        """
        if sanitize_column_names:
            columns = ', '.join([f'"{self.sanitize_column_name(col)}"' for col in df.columns])
        else:
            columns = ', '.join([f'"column_{i+1}"' for i in range(len(df.columns))])
        values_list = []
        
        for row in df.values:
            formatted_values = [self.format_value(value) for value in row]
            values_list.append(f"({', '.join(formatted_values)})")

        values = ', '.join(values_list)
        insert_data_sql = f'INSERT INTO "{self.schema_name}"."{table_name}" ({columns}) VALUES {values};'

        if not skip_duplicates:
            insert_data_sql = f'{insert_data_sql}\n\n{self.sql_remove_duplicates(table_name, self.sanitize_column_name(df.columns[0]))}'
        
        return insert_data_sql
    
    def write_sql_file(self, sql_statements, file_path, table_name=None):
        """
        Writes the SQL statements to a file.
        
        Args:
            sql_statements (str): The SQL statements to write.
            file_path (str): The path to the output file.
            table_name (str): The name of the table. If not provided, the table name will be inferred from the file path.
        """
        if table_name is None:
            folder_name = os.path.dirname(file_path)
            table_name = os.path.basename(folder_name)

        with open(file_path, 'w', encoding="utf-8") as f:
            f.write(f'{sql_statements}\n')

class SearchFolders:
    """
    A class that provides methods to search for folders and files in a given directory.
    """

    def __init__(self):
        pass

    def get_folders(self, folder_path):
        """
        Returns a list of folders in the specified directory.

        Parameters:
        - folder_path (str): The path of the directory to search for folders.

        Returns:
        - list: A list of folder names.
        """
        folders = [f for f in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, f))]
        return folders
    
    def get_files(self, folder_path):
        """
        Returns a list of files in the specified directory.

        Parameters:
        - folder_path (str): The path of the directory to search for files.

        Returns:
        - list: A list of file names.
        """
        files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        return files
    
    def search_folders(self, folder_path):
        """
        Searches for folders and files in the specified directory.

        Parameters:
        - folder_path (str): The path of the directory to search.

        Returns:
        - dict: A dictionary containing two lists: 'folders' (list of folder names) and 'files' (list of file names).
        """
        folders = self.get_folders(folder_path)
        files = self.get_files(folder_path)
        return {'folders': folders, 'files': files}
    
class ColumnDocCreator:
    """
    A class to create a .txt documentation file explaining each column in a table.
    
    Args:
        file_path (str): The path to the output .txt file.
        df (pandas.DataFrame): The DataFrame containing the data.
    """
    
    def __init__(self, file_path, df):
        self.file_path = file_path
        self.df = df

    def create_doc(self):
        """
        Creates the .txt file documenting the columns.
        """
        with open(self.file_path, 'w', encoding='utf-8') as f:
            f.write("Table Columns Documentation\n\n")
            for i, (col, dtype) in enumerate(zip(self.df.columns, self.df.dtypes), 1):
                if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
                    dtype = "INTEGER"
                f.write(f"Column_{i} ({dtype}): {col}\n")

class DataHandler:
    """
    A class that handles the processing of data folders and generates SQL files.

    Attributes:
    - data_folder (str): The path to the data folder.
    - schema_name (str): The name of the database schema.
    - handler_FF (SearchFolders): An instance of the SearchFolders class.
    - handler (ExcelDataHandler): An instance of the ExcelDataHandler class.

    Methods:
    - process_folders(): Processes the folders in the data folder, merges the data, removes duplicate rows,
      generates SQL files for each folder.
    - run(): Runs the data processing and SQL file generation.
    """

    def __init__(self, data_folder, schema_name, sheet_name=None):
        """
        Initializes a DataHandler object.

        Parameters:
        - data_folder (str): The path to the data folder.
        - schema_name (str): The name of the database schema.
        """
        self.data_folder = data_folder
        self.schema_name = schema_name
        self.handler_FF = SearchFolders()

        if sheet_name:
            self.handler = ExcelDataHandler(schema_name, sheet_name)
        else:
            self.handler = ExcelDataHandler(schema_name)

    def process_folders(self, skip_duplicates=False, sanitize_column_names=False):
        """
        Processes the folders in the data folder, merges the data, removes duplicate rows,
        and generates SQL files for each folder.

        
        """
        for folder in self.handler_FF.get_folders(self.data_folder):
            folder_path = os.path.join(self.data_folder, folder)
            print(f"Processing folder: {folder_path}")
            
            merged_df = self.handler.search_and_merge_xlsx(folder_path)

            if not skip_duplicates:
                merged_df = self.handler.remove_duplicate_rows(merged_df)  # To remove the duplicate rows from "Response ID" column

            table_name = folder
            create_table_sql = self.handler.create_table_sql(table_name, merged_df, sanitize_column_names)
            insert_data_sql = self.handler.insert_data_sql(table_name, merged_df, skip_duplicates, sanitize_column_names)

            sql_file_path_create_table_sql = os.path.join(folder_path, f'create_table_{table_name}.sql')
            sql_file_path_insert_data_sql = os.path.join(folder_path, f'insert_data_{table_name}.sql')
            
            with open(sql_file_path_create_table_sql, "w", encoding="utf-8") as f:
                f.write(create_table_sql)

            with open(sql_file_path_insert_data_sql, "w", encoding="utf-8") as f:
                f.write(insert_data_sql)

            # Create the documentation file for columns
            doc_file_path = os.path.join(folder_path, f'{table_name}_columns.txt')
            column_doc_creator = ColumnDocCreator(doc_file_path, merged_df)
            column_doc_creator.create_doc()

            print(f"SQL files and documentation created for {table_name}")

    def run(self, skip_duplicates=False, sanitize_column_names=False):
        """
        Runs the data processing and SQL file generation.
        """
        self.process_folders(skip_duplicates, sanitize_column_names)


""" 
This is an example of how to use the DataHandler class to process data folders and generate SQL files.

There are some points to consider:
1. The data folder should contain subfolders, each containing Excel files.
    As an example, we have the following structure:
    
    - Example_data
        - Folder_1 -> This will be treated as a table in the database.
            - File_1.xlsx
            - File_2.xlsx
        - Folder_2
            - File_3.xlsx
            - File_4.xlsx

2. Each subfolder will be treated as a separate table in the database.
3. The output SQL files will be created in the same folder as the Excel files.

Enjoy! :D
"""
if __name__ == "__main__":
    data_folder = r"E:\Salman\dataset" # The path to the data folder
    schema_name = "hajj 2024" # The name of the database schema (Note that the schema must already exist in the database)
    sheet_name = "Sheet1" # The name of the Excel sheet to read (if not provided, it will default to "Raw Data")

    DataHandler(data_folder, schema_name, sheet_name).run(skip_duplicates=True, sanitize_column_names=False)
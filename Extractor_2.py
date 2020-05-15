import logging
import yaml
import pandas as pd
import sys
from datetime import datetime
import os
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('data-extractor').setLevel(level=logging.WARNING)
logger = logging.getLogger(__name__)

def start(config):
    print("Initiating data read process. Please wait..")
    # Getting parameters from config file
    first_file_name = config['first_file_name']
    first_file_sep = config['first_file_sep']
    
    second_file_name = config['second_file_name']
    second_file_sep = config['second_file_sep']
    
    ref_col_name = config['ref_col_name']
    
    # Reading first file
    df1 = pd.read_csv(first_file_name, sep=first_file_sep, encoding = "ISO-8859-1" , error_bad_lines=False,
                               warn_bad_lines=False, low_memory=False,  dtype=object)
    
    df1['ADDRESS1'].replace(np.nan,'NA')
    df1['ADDRESS2'].replace(np.nan,'NA')
    
    # Droping the duplicate id records
    df1.drop_duplicates(subset=[ref_col_name], keep = 'first', inplace = True)
    print(len(df1.columns.values))
    df2 = pd.read_csv(second_file_name, sep=second_file_sep, encoding = "ISO-8859-1" , error_bad_lines=False,
                               warn_bad_lines=False, dtype=object)
    
    # Deleting the files if exists
    if os.path.exists("ZYME_ID.csv"):
        os.remove("ZYME_ID.csv")
    if os.path.exists("ADDRESS_1_2.csv"):
        os.remove("ADDRESS_1_2.csv")
    if os.path.exists("ID_ADDRESS_1_2.csv"):
        os.remove("ID_ADDRESS_1_2.csv")
        
    df1.columns = [str(col) + '_X' for col in df1.columns]
    
    # creating a empty bucket to save result
    df_result = pd.DataFrame(columns=(df1.columns.append(df2.columns)))
    df_result.to_csv("ZYME_ID.csv", index_label=False, header=True)
    df_result.to_csv("ADDRESS_1_2.csv", index_label=False, header=True)
    df_result.to_csv("ID_ADDRESS_1_2.csv", index_label=False, header=True)
    
    # deleting df2 to save memory
    del(df2)
    
    # Reading second file in chunks to avoid memory error
    reader = pd.read_csv(second_file_name, sep=second_file_sep, encoding = "ISO-8859-1" , error_bad_lines=False,
                               warn_bad_lines=False, chunksize=10000, dtype=object) # chunksize depends with you colsize
    
    [preprocess(df1, df2, ref_col_name) for df2 in reader]
    del(df1)
    print("\nFile merging process complete.")
    
def preprocess(df1, df2, ref_col_name):
    df2['MAster_address1'].replace(np.nan,'NA')
    df2['Master_address2'].replace(np.nan,'NA')
    
    df2.drop_duplicates(subset=[ref_col_name,"MAster_address1","Master_address2"], keep = 'first', inplace = True)
    
    merged_frame_id_add = pd.DataFrame.merge(df1, df2, right_on=[ref_col_name,"MAster_address1","Master_address2"],
                                              left_on=[ref_col_name  + "_X","ADDRESS1_X","ADDRESS2_X"],
                                              how='inner')
    merged_frame_id_add.to_csv("ID_ADDRESS_1_2.csv", mode="a", header=False, index=False)
    
    df2.drop_duplicates(subset=["MAster_address1","Master_address2"], keep = 'first', inplace = True)
    
    merged_frame_add = pd.DataFrame.merge(df1, df2, right_on=["MAster_address1","Master_address2"],
                                          left_on=["ADDRESS1_X","ADDRESS2_X"],                                                  
                                              how='inner')

    merged_frame_add.to_csv("ADDRESS_1_2.csv", mode="a", header=False, index=False)
  
    df2.drop_duplicates(subset=[ref_col_name], keep = 'first', inplace = True)
    
    merged_frame = pd.merge(df1, df2, left_on=[ref_col_name + "_X"], right_on=[ref_col_name], how='inner')
    
    merged_frame.to_csv("ZYME_ID.csv", mode="a", header=False, index=False)
    sys.stdout.write("\r" + str(datetime.now()))
    sys.stdout.flush
    
    
if __name__ == "__main__":
    with open('config.yml', 'rb') as f:
        config = yaml.load(f)
    start(config)
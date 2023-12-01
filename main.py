
from clean_upload_data import *
from get_sharepoint_files import get_username, get_sharepoint_folder, preprocess_files
from qa_recalls_functions import QaRecallsFilesManager
from dotenv import load_dotenv
import os

load_dotenv() # Load environment variables

file_path = os.getenv('FILE_PATH') # Local path where files were stored
results_path = os.getenv('RESULTS_PATH') # Local path where results will be stored
# username and password
username, password = get_username()
# Specify the document library or folder and file name
site_url = "https://share.amazon.com/sites/sjocopsqa/"

# S3 required data
bucket = 'quicksigth-dashboards'
bucket_destionation_path = os.getenv('BUKET_DESTINATION_PATH')

#Get the folder object
folder = get_sharepoint_folder(username, password, site_url)

all_dataframes = preprocess_files(folder)

global_recalls_compiled_NA_tt_level = all_dataframes['Global Recalls Compiled File NA_TT Level']


NA_Private_Brands_Recalls_MW_Compiled_File_TT_Level = all_dataframes['NA Private Brands Recalls and Market Withdrawals compiled file_TT Level']
NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause = all_dataframes['NA Private Brands Recalls and Market Withdrawals compiled file_Raw Data Errors & Root Cause']

NA_Recalls_Compiled_File_TT_Level = all_dataframes['NA Recalls Compiled File_TT Level']
NA_Recalls_Compiled_File_ASIN_Level = all_dataframes['NA Recalls Compiled File_ASIN Level']
NA_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause = all_dataframes['NA Recalls Compiled File_Raw Data Errors & Root Cause']

MW_Compiled_File_MW_Data = all_dataframes['MW Compiled File_MW Data']
MW_Compiled_File_Raw_Data_Errors_Root_Cause = all_dataframes['MW Compiled File_Raw Data Errors & Root Cause']

Global_Recalls_Compiled_File_LATAM_TT_Level = all_dataframes['Global Recalls Compiled File LATAM_TT Level']
Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause = all_dataframes['Global Recalls Compiled File LATAM_Raw Data Errors & Root Cause']

NA_LATAM_Non_Actionable_Recalls_Compiled_File_TT_Level = all_dataframes['NA-LATAM Non-Actionable Recalls Compiled File_TT Level']
NA_LATAM_Non_Actionable_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause = all_dataframes['NA-LATAM Non-Actionable Recalls Compiled File_Raw Data Errors & Root Cause']

LATAM_Recalls_Compiled_File_TT_Level = all_dataframes['LATAM Recalls Compiled File_TT Level']
LATAM_Recalls_Compiled_File_ASIN_Level = all_dataframes['LATAM Recalls Compiled File_ASIN Level']
LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause = all_dataframes['LATAM Recalls Compiled File_Raw Data Errors & Root Cause']



# # Save file in filepath
# save_csv_file(LATAM_Recalls_Compiled_File_TT_Level, file_path, 'LATAM_Recalls_Compiled_File_TT_Level')
# save_csv_file(LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause, file_path, 'LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause')

# save_csv_file(Global_Recalls_Compiled_File_LATAM_TT_Level, file_path, 'Global_Recalls_Compiled_File_LATAM_TT_Level')
# save_csv_file(Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause, file_path, 'Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause')

# save_csv_file(MW_Compiled_File_MW_Data, file_path, 'MW_Compiled_File_MW_Data')
# save_csv_file(MW_Compiled_File_Raw_Data_Errors_Root_Cause, file_path, 'MW_Compiled_File_Raw_Data_Errors_Root_Cause')

# save_csv_file(NA_LATAM_Non_Actionable_Recalls_Compiled_File_TT_Level, file_path, 'NA_LATAM_Non_Actionable_Recalls_Compiled_File_TT_Level')
# save_csv_file(NA_LATAM_Non_Actionable_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause, file_path, 'NA_LATAM_Non_Actionable_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause')

# save_csv_file(NA_Private_Brands_Recalls_MW_Compiled_File_TT_Level, file_path, 'NA_Private_Brands_Recalls_MW_Compiled_File_TT_Level')
# save_csv_file(NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause, file_path, 'NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause')


# # Read files LATAM RECALLS
# LATAM_Recalls_Compiled_File_TT_Level = pd.read_csv(f'{file_path}LATAM_Recalls_Compiled_File_TT_Level.csv')
# LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause = pd.read_csv(f'{file_path}LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause.csv')
#
# # Read files MW
# MW_Compiled_File_MW_Data = pd.read_csv(f'{file_path}MW_Compiled_File_MW_Data.csv')
# MW_Compiled_File_Raw_Data_Errors_Root_Cause = pd.read_csv(f'{file_path}MW_Compiled_File_Raw_Data_Errors_Root_Cause.csv')
#
# # Read global LATAM
# Global_Recalls_Compiled_File_LATAM_TT_Level = pd.read_csv(f'{file_path}Global_Recalls_Compiled_File_LATAM_TT_Level.csv')
# Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause = pd.read_csv(f'{file_path}Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause.csv')
#
# # Read NA LATAM Non Actionable
# NA_LATAM_Non_Actionable_Recalls_Compiled_File_TT_Level = pd.read_csv(f'{file_path}NA_LATAM_Non_Actionable_Recalls_Compiled_File_TT_Level.csv')
# NA_LATAM_Non_Actionable_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause = pd.read_csv(f'{file_path}NA_LATAM_Non_Actionable_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause.csv')
#
# # Read NA Private Brands
# NA_Private_Brands_Recalls_MW_Compiled_File_TT_Level = pd.read_csv(f'{file_path}NA_Private_Brands_Recalls_MW_Compiled_File_TT_Level.csv')
# NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause = pd.read_csv(f'{file_path}NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause.csv')



# Create an instance of QaRecallsFilesManager
qa_recalls_manager_instance = QaRecallsFilesManager()


try:
    # Call function
    latam_recalls_compiled_calculated_values = qa_recalls_manager_instance.calcs_for_LATAM_LATAM_Recalls_Compiled_File(
        LATAM_Recalls_Compiled_File_TT_Level, LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause)

    mw_compiled_calculated_values = qa_recalls_manager_instance.calcs_for_MW_Compiled_File(
        MW_Compiled_File_MW_Data, MW_Compiled_File_Raw_Data_Errors_Root_Cause)

    global_latam_calculated_values = qa_recalls_manager_instance.calcs_for_global_latam(
        Global_Recalls_Compiled_File_LATAM_TT_Level,Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause)

    na_latam_non_act_calculated_values = qa_recalls_manager_instance.calcs_for_na_latam_non_actionable(
        NA_LATAM_Non_Actionable_Recalls_Compiled_File_TT_Level,
        NA_LATAM_Non_Actionable_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause)

    na_private_brands_recalls_mw_compiled_calculated_values = qa_recalls_manager_instance.\
        calcs_for_na_private_brands_recalls_mw(NA_Private_Brands_Recalls_MW_Compiled_File_TT_Level,
                                               NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause)


except Exception as e:
    print('Error in Calculations:', e)
    pass

else:
    # Save file in results filepath
    save_csv_file(latam_recalls_compiled_calculated_values, results_path, 'LATAM_Recalls_Compiled_File_Calculated_Values')
    save_csv_file(mw_compiled_calculated_values, results_path, 'MW_Compiled_File_Calculated_Values')
    save_csv_file(global_latam_calculated_values, results_path, 'Global_Recalls_Compiled_File_Calculated_Values')
    save_csv_file(na_latam_non_act_calculated_values, results_path, 'NA_LATAM_Non_Actionable_Recalls_Compiled_File_Calculated_Values')
    save_csv_file(na_private_brands_recalls_mw_compiled_calculated_values, results_path,
                  'NA_Private_Brands_Recalls_MW_Compiled_File_Calculated_Values')

    # Upload to s3 bucket
    upload_to_s3(bucket, results_path + 'LATAM_Recalls_Compiled_File_Calculated_Values.csv', bucket_destionation_path + 'LATAM_Recalls_Compiled_File_Calculated_Values.csv')
    upload_to_s3(bucket, results_path + 'MW_Compiled_File_Calculated_Values.csv', bucket_destionation_path + 'MW_Compiled_File_Calculated_Values.csv')
    upload_to_s3(bucket, results_path + 'Global_Recalls_Compiled_File_Calculated_Values.csv', bucket_destionation_path + 'Global_Recalls_Compiled_File_Calculated_Values.csv')
    upload_to_s3(bucket, results_path + 'NA_LATAM_Non_Actionable_Recalls_Compiled_File_Calculated_Values.csv', bucket_destionation_path + 'NA_LATAM_Non_Actionable_Recalls_Compiled_File_Calculated_Values.csv')
    upload_to_s3(bucket, results_path + 'NA_Private_Brands_Recalls_MW_Compiled_File_Calculated_Values.csv', bucket_destionation_path + 'NA_Private_Brands_Recalls_MW_Compiled_File_Calculated_Values.csv')


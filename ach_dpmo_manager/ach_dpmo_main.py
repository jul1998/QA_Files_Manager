
from clean_upload_data import *
from get_sharepoint_files import get_username, get_sharepoint_folder, preprocess_files
from clean_upload_data import concat_df
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv() # Load environment variables

file_path = os.getenv('FILE_PATH') # Local path where files were stored
results_path = os.getenv('RESULTS_PATH') # Local path where results will be stored
# username and password
username, password = get_username()
# Specify the document library or folder and file name
site_url = "https://share.amazon.com/sites/RE-Investigations, Scrubs and Reinstatement Metric data/"

# S3 required data
bucket = 'quicksigth-dashboards'
bucket_destionation_path = os.getenv('BUKET_DESTINATION_PATH_ACH')

#Get the folder object
folder_url = os.getenv('FOLDER_PATH_ACH')
folder = get_sharepoint_folder(username, password, site_url, folder_url)

all_dataframes = preprocess_files(folder)

def run_ach_qa_manager():

    #create a dataframe the tab RE- Investigations, Scrubs and Reinstatement Metric data_Reinstatetment-DPMO
    RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO = all_dataframes['RE- Investigations, Scrubs and Reinstatement Metric data_Raw Data - Reinstatetment']


    RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO = replace_commas_with_semicolon(RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO)
    # Replace \n by a space in dataframe
    RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO.replace('\n', '', regex=True)

    Year_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Year']
    Quarter_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Quarter']
    mon_num_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['mon_num']
    Month_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Month']
    Week_No_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Week No']
    MP_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['MP']
    SIM_or_TT_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['SIM or TT']
    Error_Parameter_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Error Parameter']
    Error_Parameter_Score_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Error parameter score']
    Points_Achieved_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Points achieved']
    Max_Points_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Max points']
    QA_Percentage_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['QA%']
    DPMO_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['DPMO']
    Reviewer_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Reviewer']
    Auditor_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Auditor']
    Callouts_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Callouts']
    SIM_Ops_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['SIM (Ops)']
    Case_SIM_Creation_Date_df = RE_Investigations_Scrubs_and_Reinstatement_Metric_data_Reinstatetment_DPMO['Case/SIM creation date']


    # Create a new dataframe with the new columns
    new_df = pd.DataFrame({'Year': Year_df, 'Quarter': Quarter_df, 'mon_num': mon_num_df, 'Month': Month_df,
                              'Week No': Week_No_df, 'MP': MP_df, 'SIM or TT': SIM_or_TT_df, 'Error Parameter': Error_Parameter_df,
                                'Error parameter score': Error_Parameter_Score_df, 'Points achieved': Points_Achieved_df,
                                'Max points': Max_Points_df, 'Reviewer': Reviewer_df,
                                'Auditor': Auditor_df, 'Callouts': Callouts_df, 'SIM (Ops)': SIM_Ops_df,
                                'Case/SIM creation date': Case_SIM_Creation_Date_df})

    save_csv_file(new_df, results_path, 'Final_ACH_Metric_Data')
    upload_to_s3(bucket, results_path + 'Final_ACH_Metric_Data.csv',
                 bucket_destionation_path + 'Final_ACH_Metric_Data.csv')





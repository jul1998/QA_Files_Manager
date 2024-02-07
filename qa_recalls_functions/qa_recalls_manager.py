import pandas as pd
from clean_upload_data import replace_commas_with_semicolon

class QaRecallsFilesManager:

    def calcs_for_LATAM_LATAM_Recalls_TT_level_Compiled_File(self, LATAM_Recalls_Compiled_File_TT_Level):
        """
        This function calculates the following metrics for LATAM_Recalls_Compiled_File_TT_Level:
        :return:  LATAM_Recalls_Compiled_File_TT_Level
        """

        # merged_df_Latam_recalls_compiled = pd.merge(
        #     LATAM_Recalls_Compiled_File_TT_Level,
        #     LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause,
        #     how='left',
        #     on='Specialist'
        # )

        # select data only where year is >= 2023
        LATAM_Recalls_Compiled_File_TT_Level = LATAM_Recalls_Compiled_File_TT_Level[LATAM_Recalls_Compiled_File_TT_Level['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        LATAM_Recalls_Compiled_File_TT_Level = replace_commas_with_semicolon(LATAM_Recalls_Compiled_File_TT_Level)

        # Replace \n by a space in dataframe
        LATAM_Recalls_Compiled_File_TT_Level = LATAM_Recalls_Compiled_File_TT_Level.replace('\n', '', regex=True)


        # Login level: (Y / Z) * 100
        qa_score_percentage_login_level = round((LATAM_Recalls_Compiled_File_TT_Level['Associate Pts Achieved'] /
                                                 LATAM_Recalls_Compiled_File_TT_Level['Associate Max Achieved']) * 100, 2).fillna(0).astype(float)

        # DPMO: (1 - QA Score%) * 10^6
        dpmo = round((1 - qa_score_percentage_login_level / 100) * 1000000)

        # Columns from compiled> (BI)
        tp_asins = LATAM_Recalls_Compiled_File_TT_Level['Yanked'].fillna(0).astype(int)

        # column BK
        false_positive_asins = LATAM_Recalls_Compiled_File_TT_Level['Associate Level Overpulls'].fillna(0).astype(int)

        # column BJ
        false_negative_asins = LATAM_Recalls_Compiled_File_TT_Level['Associate Level Underpulls'].fillna(0).astype(int)

        # Audit Sample = True Positive ASINs + False Positive ASINs (BI + BK)
        audit_sample = (tp_asins + false_positive_asins).fillna(0).astype(int)

        # False Positive Rate (Percentage of Over-pulled ASINs): False Positive ASINs / Audit Sample
        false_positive_rate = ((false_positive_asins / audit_sample) * 100).fillna(0).astype(float)

        # False Negative Rate (Percentage of Under-pulled ASINs): False Negative ASINs / ( Audit Sample - FP + FN )
        false_negative_rate = ((false_negative_asins / (
                    audit_sample - false_positive_asins + false_negative_asins)) * 100).fillna(0).astype(float)

        # FP DPMO: { 100- [ 100% - ( FP / Audit sample ) % ] } * 1000000 in percentage
        fp_dpmo = ((100 - (100 - (false_positive_asins / audit_sample))) * 1000000).fillna(0).astype(int)

        # FN DPMO: { 100- [ 100% - ( FN / Audit sample - FP + FN ) % ] } * 1000000, in which:
        fn_dpmo = ((100 - (
                100 - (false_negative_asins / (audit_sample - false_positive_asins + false_negative_asins)))) * 1000000).fillna(
            0).astype(int)

        # -------------Deffect summary visualization ----------------
        specialist_tt_level = LATAM_Recalls_Compiled_File_TT_Level['Specialist'].fillna("NA")  # Associate: column M
        year = LATAM_Recalls_Compiled_File_TT_Level['Year'].fillna(pd.NaT)  # Year: column B
        marketplace_tt_level = LATAM_Recalls_Compiled_File_TT_Level['MP'].fillna("NA")  # Marketplace: column I
        week = LATAM_Recalls_Compiled_File_TT_Level['Completed Week'].fillna(pd.NaT)  # Week: column D
        date_assigned = pd.to_datetime(LATAM_Recalls_Compiled_File_TT_Level['Date Assigned']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())
        date_completed = pd.to_datetime(
            LATAM_Recalls_Compiled_File_TT_Level['Date Completed']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())  # Date Completed: column G
        tt_URL_tt_level = LATAM_Recalls_Compiled_File_TT_Level['TT URL'].fillna("NA")  # TT Link: column K
        feedback = LATAM_Recalls_Compiled_File_TT_Level['Feedback'].fillna("NA")  # Feedback: column CJ
        is_biased = LATAM_Recalls_Compiled_File_TT_Level['Unbiased/Biased'].fillna("NA")  # Is Biased: column E
        recalls_pts_achieved = LATAM_Recalls_Compiled_File_TT_Level['Recall Pts Achieved'].fillna(0).astype(
            int)  # Recall Pts Achieved: column CO
        recalls_max_achieved = LATAM_Recalls_Compiled_File_TT_Level['Recall Max Achieved'].fillna(0).astype(
            int)  # Recall Max Achieved: column CP



        # Create dataframe
        latam_recalls_compiled_tt_level_calculated_values = pd.DataFrame({'Source': 'LATAM_Recalls_TT_Level_Compiled_File',
                                                                 'TT_URL': tt_URL_tt_level,
                                                                 'Specialist': specialist_tt_level,
                                                                'Marketplace': marketplace_tt_level,
                                                                'Feedback': feedback,
                                                                'Is_Biased': is_biased,
                                                                'Date_Assigned': date_assigned,
                                                                 'Date_Completed': date_completed,
                                                                 'QA_Score_%': qa_score_percentage_login_level,
                                                                 'DPMO': dpmo,
                                                                 'TP_ASINs': tp_asins,
                                                                 'False_Positive_ASINs': false_positive_asins,
                                                                 'False_Negative_ASINs': false_negative_asins,
                                                                 'Audit_Sample': audit_sample,
                                                                 'False_Positive_Rate': false_positive_rate,
                                                                 'False_Negative_Rate': false_negative_rate,
                                                                 'FP_DPMO': fp_dpmo,
                                                                 'FN_DPMO': fn_dpmo,
                                                                'Recalls_Pts_Achieved': recalls_pts_achieved,
                                                                'Recalls_Max_Achieved': recalls_max_achieved,
                                                                          'Error Type': "NA",
                                                                          'QC Parameter Error': "NA",
                                                                          'Primary RC': "NA",
                                                                          'Secondary RC': "NA",
                                                                          'Tertiary RC': "NA"

                                                                     })



        return latam_recalls_compiled_tt_level_calculated_values

    def calcs_for_LATAM_LATAM_Recalls_raw_errors_Compiled_File(self,
                                                             LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause):
        """
        This function calculates the following metrics for LATAM_Recalls_Compiled_File_TT_Level:
        :return:  LATAM_Recalls_Compiled_File_raw_errors
        """

        # Create a year column in the dataframe
        LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['Year'] = pd.to_datetime(
            LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['Date Completed']).dt.year


        # select data only where year is >= 2023
        LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause[
            LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause = replace_commas_with_semicolon(LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause)

        # Replace \n by a space in dataframe
        LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause.replace('\n', '', regex=True)

        # -------------Root cause visualization ----------------
        tt_URL_raw_errors = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['TT URL'].fillna(
            "NA")  # TT URL: column A
        specialist= LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['Specialist'].fillna("NA")
        marketplace = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['MP'].fillna("NA")
        is_biased = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['Unbiased/Biased'].fillna("NA")  # Is Biased: column E
        date_completed = pd.to_datetime(
            LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['Date Completed']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())  #
        error_type = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['Type of Error'].fillna(
            "NA")  # Error Type: column CI

        qc_parameter_error = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['QC Parameter Error'].fillna(
            "NA")  # QC Parameter Error: column CG
        primary_rc = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['ROOT CAUSE PRIMARY'].fillna(
            "NA")  # Primary RC: column CJ
        secondary_rc = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['ROOT CAUSE SECONDARY'].fillna(
            "NA")  # Secondary RC: column CK
        tertiary_rc = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['ROOT CAUSE TERTIARY'].fillna(
            "NA")  # Tertiary RC: column CL

        # Create dataframe

        latam_recalls_compiled_raw_errors_calculated_values = pd.DataFrame({'Source': 'LATAM_Recalls_raw_errors_Compiled_File',
                                                                            'TT_URL': tt_URL_raw_errors,
                                                                            'Specialist': specialist,
                                                                            'Marketplace': marketplace,
                                                                            'Feedback': "NA",
                                                                            'Is_Biased': is_biased,
                                                                            'Date_Assigned':  pd.to_datetime('1900-01-01').date(),
                                                                            'Date_Completed': date_completed,
                                                                            'QA_Score_%': 0.0,
                                                                            'DPMO': 0.0,
                                                                            'TP_ASINs': 0,
                                                                            'False_Positive_ASINs': 0,
                                                                            'False_Negative_ASINs': 0,
                                                                            'Audit_Sample': 0,
                                                                            'False_Positive_Rate': 0.0,
                                                                            'False_Negative_Rate': 0.0,
                                                                            'FP_DPMO': 0.0,
                                                                            'FN_DPMO': 0.0,
                                                                            'Recalls_Pts_Achieved':0,
                                                                            'Recalls_Max_Achieved': 0,
                                                                            'Error Type': error_type,
                                                                            'QC Parameter Error': qc_parameter_error,
                                                                            'Primary RC': primary_rc,
                                                                            'Secondary RC': secondary_rc,
                                                                            'Tertiary RC': tertiary_rc
                                                                            })


        return latam_recalls_compiled_raw_errors_calculated_values



    def calcs_for_MW_TT_level_Compiled_File(self, MW_Compiled_File_MW_Data):
        """
        This function calculates the following metrics for MW_Compiled_File_MW_Data:
        :return: MW_Compiled_File_MW_Data
        """

        # merged_df_mw_compiled = pd.merge(
        #     MW_Compiled_File_MW_Data,
        #     MW_Compiled_File_Raw_Data_Errors_Root_Cause,
        #     how='left',
        #     on='TT URL'
        # )

        # select data only where year is >= 2023
        MW_Compiled_File_MW_Data = MW_Compiled_File_MW_Data[MW_Compiled_File_MW_Data['Year'] >= 2023]



        # Replace commas by semmincolons in dataframe
        MW_Compiled_File_MW_Data = replace_commas_with_semicolon(MW_Compiled_File_MW_Data)

        # Replace \n by a space in dataframe
        MW_Compiled_File_MW_Data = MW_Compiled_File_MW_Data.replace('\n', ' ', regex=True)

        # QA Score %: Total points achieved / Total Max Points
        # Login
        # level: (N / O)
        qa_score_percentage_login_level = round((MW_Compiled_File_MW_Data['Associate Pts Achieved'] /
                                                 MW_Compiled_File_MW_Data['Associate Max Achieved']) * 100, 2).fillna(0).astype(float)

        # DPMO: (1-QA Score%)*10^6
        dpmo = round((1 - qa_score_percentage_login_level / 100) * 1000000)

        # Columns from compiled> (AJ)
        tp_asins = MW_Compiled_File_MW_Data['Yanked'].fillna(0).astype(int)

        # FP: column AL
        false_positive_asins = MW_Compiled_File_MW_Data['Associate Level Overpulls'].fillna(0).astype(int)

        # FN: column AK
        false_negative_asins = MW_Compiled_File_MW_Data['Associate Level Underpulls'].fillna(0).astype(int)

        # True Positive ASINs + False Positive ASINs (AJ + AL)
        audit_sample = (tp_asins + false_positive_asins).fillna(0).astype(int)

        # False Positive Rate (Percentage of Over-pulled ASINs): False Positive ASINs / Audit Sample
        false_positive_rate = ((false_positive_asins / audit_sample) * 100).fillna(0).astype(float)

        # False Negative Rate (Percentage of Under-pulled ASINs): False Negative ASINs / ( Audit Sample - FP + FN )
        false_negative_rate = ((false_negative_asins / (
                    audit_sample - false_positive_asins + false_negative_asins)) * 100).fillna(0).astype(float)

        # FP DPMO: { 1- [ 100% - ( FP / Audit sample ) % ] } * 1000000 in percentage
        fp_dpmo = ((100 - (100 - (false_positive_asins / audit_sample))) * 1000000).fillna(0).astype(int)

        # FN DPMO: { 1- [ 100% - ( FN / Audit sample - FP + FN ) % ] } * 1000000, in which:
        fn_dpmo = ((100 - (
                100 - (false_negative_asins / (audit_sample - false_positive_asins + false_negative_asins)))) * 1000000).fillna(0).astype(int)

        # -------------Deffect summary visualization ----------------

        """
        Fields and columns for the Defect Summary visual:

        YEAR: column A
        TT URL mw data: column  H
        Specialist mw data: column J
        Week completed: column C
        Date Assigned: column D
        Date Completed: column E
        Error Type: column L
        Feedback: column F
        Marketplace: column G
        QC Parameter Error: column J
        Primary RC: column M
        Secondary RC: column N
        Tertiary RC: column O
        """
        tt_url_mw_data = MW_Compiled_File_MW_Data['TT URL'].fillna("NA")
        specialist_mw_data = MW_Compiled_File_MW_Data['Specialist'].fillna("NA")
        date_assigned = pd.to_datetime(MW_Compiled_File_MW_Data['Date Assigned']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())
        date_completed = pd.to_datetime(MW_Compiled_File_MW_Data['Date Completed']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())
        marketplace = MW_Compiled_File_MW_Data['MP'].fillna("NA")
        feedback = MW_Compiled_File_MW_Data['Feedback'].fillna("NA")

        recalls_pts_achieved = MW_Compiled_File_MW_Data['MW Pts Achieved'].fillna(0).astype(
            int)  # Recall Pts Achieved: column CO
        recalls_max_achieved = MW_Compiled_File_MW_Data['MW Max Achieved'].fillna(0).astype(
            int)  # Recall Max Achieved: column CP

        # Create dataframe
        mw_compiled_tt_level_calculated_values = pd.DataFrame({'Source': 'MW_TT_level_Compiled_File',
                                                               'TT_URL': tt_url_mw_data,
                                                               'Specialist': specialist_mw_data,
                                                               'Marketplace': marketplace,
                                                               'Feedback': feedback,
                                                               'Is_Biased': "NA",
                                                               'Date_Assigned': date_assigned,
                                                               'Date_Completed': date_completed,
                                                               'QA_Score_%': qa_score_percentage_login_level,
                                                               'DPMO': dpmo,
                                                               'TP_ASINs': tp_asins,
                                                               'False_Positive_ASINs': false_positive_asins,
                                                               'False_Negative_ASINs': false_negative_asins,
                                                               'Audit_Sample': audit_sample,
                                                               'False_Positive_Rate': false_positive_rate,
                                                               'False_Negative_Rate': false_negative_rate,
                                                               'FP_DPMO': fp_dpmo,
                                                               'FN_DPMO': fn_dpmo,
                                                               'Recalls_Pts_Achieved': recalls_pts_achieved,
                                                               'Recalls_Max_Achieved': recalls_max_achieved,
                                                               'Error Type': "NA",
                                                               'QC Parameter Error': "NA",
                                                               'Primary RC': "NA",
                                                               'Secondary RC': "NA",
                                                               'Tertiary RC': "NA"

                                                               })

        # print(mw_compiled_calculated_values.head(), mw_compiled_calculated_values.shape)
        return mw_compiled_tt_level_calculated_values

    def calcs_for_MW_raw_errors_Compiled_File(self, MW_Compiled_File_Raw_Data_Errors_Root_Cause):
        """
        This function calculates the following metrics for MW_Compiled_File_Raw_Data_Errors_Root_Cause:
        :return: MW_Compiled_File_Raw_Data_Errors_Root_Cause_Calculated_Values
        """

        # Create A Year column based on the date column
        MW_Compiled_File_Raw_Data_Errors_Root_Cause['Year'] = pd.to_datetime(MW_Compiled_File_Raw_Data_Errors_Root_Cause['Date Completed']).dt.year

        # select data only where year is >= 2023
        MW_Compiled_File_Raw_Data_Errors_Root_Cause = MW_Compiled_File_Raw_Data_Errors_Root_Cause[MW_Compiled_File_Raw_Data_Errors_Root_Cause['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        MW_Compiled_File_Raw_Data_Errors_Root_Cause = replace_commas_with_semicolon(MW_Compiled_File_Raw_Data_Errors_Root_Cause)

        # Replace \n by a space in dataframe
        MW_Compiled_File_Raw_Data_Errors_Root_Cause = MW_Compiled_File_Raw_Data_Errors_Root_Cause.replace('\n', ' ', regex=True)

        tt_URL_raw_errors = MW_Compiled_File_Raw_Data_Errors_Root_Cause['TT URL'].fillna("NA")
        specialist = MW_Compiled_File_Raw_Data_Errors_Root_Cause['Specialist'].fillna("NA")
        marketplace = MW_Compiled_File_Raw_Data_Errors_Root_Cause['MP'].fillna("NA")
        date_completed = pd.to_datetime(MW_Compiled_File_Raw_Data_Errors_Root_Cause['Date Completed']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())
        error_type = MW_Compiled_File_Raw_Data_Errors_Root_Cause['Type of Error'].fillna("NA")
        qc_parameter_error = MW_Compiled_File_Raw_Data_Errors_Root_Cause['QC Parameter Error'].fillna("NA")
        primary_rc = MW_Compiled_File_Raw_Data_Errors_Root_Cause['ROOT CAUSE PRIMARY'].fillna("NA")
        # secondary_rc = MW_Compiled_File_Raw_Data_Errors_Root_Cause['ROOT CAUSE SECONDARY']  # These two cols do not exist in file
        # tertiary_rc = MW_Compiled_File_Raw_Data_Errors_Root_Cause['ROOT CAUSE TERTIARY']

        # Create dataframe
        mw_compiled_raw_errors_calculated_values = pd.DataFrame({'Source': 'MW_Raw_Errors_Compiled_File',
                                                                 'TT_URL': tt_URL_raw_errors,
                                                                 'Specialist': specialist,
                                                                 'Marketplace': marketplace,
                                                                 'Feedback': "NA",
                                                                 'Is_Biased': "NA",
                                                                 'Date_Assigned': pd.to_datetime('1900-01-01').date(),
                                                                 'Date_Completed': date_completed,
                                                                 'QA_Score_%': 0.0,
                                                                 'DPMO': 0.0,
                                                                 'TP_ASINs': 0,
                                                                 'False_Positive_ASINs': 0,
                                                                 'False_Negative_ASINs': 0,
                                                                 'Audit_Sample': 0,
                                                                 'False_Positive_Rate': 0.0,
                                                                 'False_Negative_Rate': 0.0,
                                                                 'FP_DPMO': 0.0,
                                                                 'FN_DPMO': 0.0,
                                                                 'Recalls_Pts_Achieved': 0,
                                                                 'Recalls_Max_Achieved': 0,
                                                                 'Error Type': error_type,
                                                                 'QC Parameter Error': qc_parameter_error,
                                                                 'Primary RC': primary_rc,
                                                                 'Secondary RC': "NA",
                                                                 'Tertiary RC': "NA"
                                                      })

        # print(mw_compiled_calculated_values.head(), mw_compiled_calculated_values.shape)
        return mw_compiled_raw_errors_calculated_values

    def calcs_for_global_latam_tt_level(self, Global_Recalls_Compiled_File_LATAM_TT_Level):
        """
        This function calculates the following metrics for LATAM_Recalls_Compiled_File_MW_Data:
        :return: Global_Recalls_Compiled_File_LATAM_Calculated_Values
        """

        # merged_df_global_latam = pd.merge(
        #     Global_Recalls_Compiled_File_LATAM_TT_Level,
        #     Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause,
        #     how='left',
        #     on='TT URL',
        # )

        # select data only where year is >= 2023
        Global_Recalls_Compiled_File_LATAM_TT_Level = Global_Recalls_Compiled_File_LATAM_TT_Level[Global_Recalls_Compiled_File_LATAM_TT_Level['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        Global_Recalls_Compiled_File_LATAM_TT_Level = replace_commas_with_semicolon(Global_Recalls_Compiled_File_LATAM_TT_Level)

        # Replace \n by a space in dataframe
        Global_Recalls_Compiled_File_LATAM_TT_Level = Global_Recalls_Compiled_File_LATAM_TT_Level.replace(r'\n', ' ', regex=True)

        # QA Score %: Total points achieved / Total Max Points
        # Login
        # level: (AA / AB)
        qa_score_percentage_login_level = round((Global_Recalls_Compiled_File_LATAM_TT_Level['Associate Pts Achieved'] /
                                                 Global_Recalls_Compiled_File_LATAM_TT_Level['Associate Max Achieved']) * 100, 2).fillna(0).astype(int)

        # DPMO: (1-QA Score%)*10^6
        dpmo = round((1 - qa_score_percentage_login_level / 100) * 1000000)

        # Columns from compiled> (AG)
        tp_asins = Global_Recalls_Compiled_File_LATAM_TT_Level['Yanked'].fillna(0).astype(int)

        # FP: column AI
        false_positive_asins = Global_Recalls_Compiled_File_LATAM_TT_Level['Associate Level Overpulls'].fillna(0).astype(int)

        # FN: column AH
        false_negative_asins = Global_Recalls_Compiled_File_LATAM_TT_Level['Associate Level Underpulls'].fillna(0).astype(int)

        # True Positive ASINs + False Positive ASINs (AG + AI)
        audit_sample = (tp_asins + false_positive_asins).fillna(0).astype(int)

        # False Positive Rate (Percentage of Over-pulled ASINs): False Positive ASINs / Audit Sample
        false_positive_rate = ((false_positive_asins / audit_sample) * 100).fillna(0).astype(float)

        # False Negative Rate (Percentage of Under-pulled ASINs): False Negative ASINs / ( Audit Sample - FP + FN )
        false_negative_rate = ((false_negative_asins / (
                    audit_sample - false_positive_asins + false_negative_asins)) * 100).fillna(0).astype(float)

        # FP DPMO: { 1- [ 100% - ( FP / Audit sample ) % ] } * 1000000 in percentage
        fp_dpmo = ((100 - (100 - (false_positive_asins / audit_sample))) * 1000000).fillna(0).astype(int)

        # FN DPMO: { 1- [ 100% - ( FN / Audit sample ) % ] } * 1000000 in percentage
        fn_dpmo = ((100 - (
                100 - (false_negative_asins / (audit_sample - false_positive_asins + false_negative_asins)))) * 1000000).fillna(0).astype(int)

        # -------------Deffect summary visualization ----------------

        """
        Fields and columns for the Defect Summary visual:
        Specialist: column M
        TT Link: column A
        TT URL tt level: column K
        Error Type: column L
        Feedback: column M
        Marketplace: column I
        QC Parameter Error: column J
        Primary RC: column M
        Secondary RC: column N
        Tertiary RC: column O
        """

        tt_url_global_latam_tt_url = Global_Recalls_Compiled_File_LATAM_TT_Level['TT URL'].fillna("NA")
        specialist_global_latam_tt_url = Global_Recalls_Compiled_File_LATAM_TT_Level['Specialist'].fillna("NA")
        date_assigned = pd.to_datetime(Global_Recalls_Compiled_File_LATAM_TT_Level['Date Assigned']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())
        date_completed = pd.to_datetime(Global_Recalls_Compiled_File_LATAM_TT_Level['Date Completed']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())

        feedback = Global_Recalls_Compiled_File_LATAM_TT_Level['Feedback'].fillna("NA")
        is_biased = Global_Recalls_Compiled_File_LATAM_TT_Level['Unbiased/Biased'].fillna("NA")
        marketplace = Global_Recalls_Compiled_File_LATAM_TT_Level['MP'].fillna("NA")

        recalls_pts_achieved = Global_Recalls_Compiled_File_LATAM_TT_Level['Recall Pts Achieved'].fillna(0).astype(int)  # Recall Pts Achieved: column CO
        recalls_max_achieved = Global_Recalls_Compiled_File_LATAM_TT_Level['Recall Max Achieved'].fillna(0).astype(int)  # Recall Max Achieved: column CP


        # Create dataframe

        global_latam_calculated_values_tt_level = pd.DataFrame({'Source': 'Global_Recalls_TT_level_Compiled_File',
                                                       'TT_URL': tt_url_global_latam_tt_url,
                                                       'Specialist': specialist_global_latam_tt_url,
                                                       'Marketplace': marketplace,
                                                       'Feedback': feedback,
                                                       'Is_Biased': is_biased,
                                                       'Date_Assigned': date_assigned,
                                                       'Date_Completed': date_completed,
                                                       'QA_Score_%': qa_score_percentage_login_level,
                                                       'DPMO': dpmo,
                                                       'TP_ASINs': tp_asins,
                                                       'False_Positive_ASINs': false_positive_asins,
                                                       'False_Negative_ASINs': false_negative_asins,
                                                       'Audit_Sample': audit_sample,
                                                       'False_Positive_Rate': false_positive_rate,
                                                       'False_Negative_Rate': false_negative_rate,
                                                       'FP_DPMO': fp_dpmo,
                                                       'FN_DPMO': fn_dpmo,
                                                       'Recalls_Pts_Achieved': recalls_pts_achieved,
                                                       'Recalls_Max_Achieved': recalls_max_achieved,
                                                       'Error Type': "NA",
                                                       'QC Parameter Error': "NA",
                                                       'Primary RC': "NA",
                                                       'Secondary RC': "NA",
                                                       'Tertiary RC': "NA"
                                                       })

        return global_latam_calculated_values_tt_level

    def calcs_for_global_latam_raw_errors(self, Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause):

        Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['Year'] = pd.to_datetime(Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['Date Completed']).dt.year

        # Create A Year column based on the date column
        Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['Year'] = pd.to_datetime(
            Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['Date Completed']).dt.year

        # select data only where year is >= 2023
        Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause[
            Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause = replace_commas_with_semicolon(
            Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause)

        # Replace \n by a space in dataframe
        Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause.replace('\n', ' ',
                                                                                                          regex=True)
        tt_url_raw_errors = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['TT URL'].fillna("NA")
        specialist_raw_errors = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['Specialist'].fillna("NA")
        marketplace_raw_errors = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['MP'].fillna("NA")
        date_completed = pd.to_datetime(Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['Date Completed']).dt.date.fillna(
            pd.to_datetime('1900-01-01').date())
        error_type = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['Type of Error'].fillna("NA")
        qc_parameter_error = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['QC Parameter Error'].fillna("NA")
        primary_rc = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['ROOT CAUSE PRIMARY'].fillna("NA")
        secondary_rc = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['ROOT CAUSE SECONDARY'].fillna(
            "NA")  # These two cols do not exist in file
        tertiary_rc = Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause['ROOT CAUSE TERTIARY'].fillna("NA")

        # Create dataframe
        global_latam_calculated_values_raw_errors = pd.DataFrame({'Source': 'Global_Recalls_raw_errors_Compiled_File',
                                                                 'TT_URL': tt_url_raw_errors,
                                                                 'Specialist': specialist_raw_errors,
                                                                 'Marketplace': marketplace_raw_errors,
                                                                 'Feedback': "NA",
                                                                 'Is_Biased': "NA",
                                                                 'Date_Assigned': pd.to_datetime('1900-01-01').date(),
                                                                 'Date_Completed': date_completed,
                                                                  'QA_Score_%': 0.0,
                                                                  'DPMO': 0.0,
                                                                  'TP_ASINs': 0,
                                                                  'False_Positive_ASINs': 0,
                                                                  'False_Negative_ASINs': 0,
                                                                  'Audit_Sample': 0,
                                                                  'False_Positive_Rate': 0.0,
                                                                  'False_Negative_Rate': 0.0,
                                                                  'FP_DPMO': 0.0,
                                                                  'FN_DPMO': 0.0,
                                                                  'Recalls_Pts_Achieved': 0,
                                                                  'Recalls_Max_Achieved': 0,
                                                                 'Error Type': error_type,
                                                                 'QC Parameter Error': qc_parameter_error,
                                                                 'Primary RC': primary_rc,
                                                                 'Secondary RC': secondary_rc,
                                                                 'Tertiary RC': tertiary_rc
                                                                 })

        return global_latam_calculated_values_raw_errors

    def calcs_for_na_latam_non_actionable(self, NA_LATAM_Non_Actionable_Recalls_Compiled_File_TT_Level,
                                          NA_LATAM_Non_Actionable_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause):
        """
        Calculations for NA LATAM Non Actionable
        :return: NA LATAM Non Actionable Calculated Values
        """

        merged_df_na_latam_non_act = pd.merge(
            NA_LATAM_Non_Actionable_Recalls_Compiled_File_TT_Level,
            NA_LATAM_Non_Actionable_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause,
            how='left',
            on='TT URL'
        )

        # select data only where year is >= 2023
        merged_df_na_latam_non_act = merged_df_na_latam_non_act[merged_df_na_latam_non_act['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        merged_df_na_latam_non_act = replace_commas_with_semicolon(merged_df_na_latam_non_act)

        # Replace \n by space in dataframe
        merged_df_na_latam_non_act = merged_df_na_latam_non_act.replace('\n', ' ', regex=True)


        # QA Score %: Total points achieved / Total Max Points
        # Login
        # level: (O / P)
        qa_score_percentage_login_level = round((merged_df_na_latam_non_act['Associate Pts Achieved'] /
                                                 merged_df_na_latam_non_act['Associate Max Achieved']) * 100, 2).fillna(0).astype(float)

        # DPMO: (1-QA Score%)*10^6
        dpmo = round((1 - qa_score_percentage_login_level / 100) * 1000000)

        # -------------Deffect summary visualization ----------------

        """
        Fields and columns for the Defect Summary visual:
        Associate: column E
        TT Link: column A
        Error Type: column K
        Marketplace: column B
        QC Parameter Error: column I
        Primary RC: column L
        Secondary RC: column M
        Tertiary RC: column N
        """

        associate_na_latam_non_act_tt_level = merged_df_na_latam_non_act['Specialist_x'].fillna("NA")
        tt_url_na_latam_non_act_tt_level = merged_df_na_latam_non_act['TT URL'].fillna("NA")
        year = merged_df_na_latam_non_act['Year'].fillna(pd.NaT)
        week = merged_df_na_latam_non_act['Completed Week_x'].fillna(pd.NaT)
        date_assigned = pd.to_datetime(merged_df_na_latam_non_act['Date Assigned']).fillna(pd.to_datetime('1900-01-01'))
        date_completed = pd.to_datetime(merged_df_na_latam_non_act['Date Completed_x']).fillna(pd.to_datetime('1900-01-01'))
        error_type = merged_df_na_latam_non_act['Type of Error'].fillna("NA")
        feedback = merged_df_na_latam_non_act['Feedback'].fillna("NA")
        marketplace = merged_df_na_latam_non_act['MP_x'].fillna("NA")
        qc_parameter_error = merged_df_na_latam_non_act['QC Parameter Error'].fillna("NA")
        primary_rc = merged_df_na_latam_non_act['ROOT CAUSE PRIMARY'].fillna("NA")
        secondary_rc = merged_df_na_latam_non_act['ROOT CAUSE SECONDARY'].fillna("NA")
        tertiary_rc = merged_df_na_latam_non_act['ROOT CAUSE TERTIARY'].fillna("NA")
        recalls_pts_achieved = merged_df_na_latam_non_act['Recall Pts Achieved'].fillna(0).astype(int)  # Recall Pts Achieved: column CO
        recalls_max_achieved = merged_df_na_latam_non_act['Recall Max Achieved'].fillna(0).astype(int)  # Recall Max Achieved: column CP


        # Create dataframe

        na_latam_non_act_calculated_values = pd.DataFrame({'Source': 'NA_LATAM_Non_Actionable_Recalls_Compiled_File',
                                                           'TT_URL_TT_Level': tt_url_na_latam_non_act_tt_level,
                                                           'Specialist_TT_Level': associate_na_latam_non_act_tt_level,
                                                           'QA Score %': qa_score_percentage_login_level,
                                                           'DPMO': dpmo,
                                                           'TP ASINs': 0,
                                                           'False Positive ASINs': 0,
                                                           'False Negative ASINs': 0,
                                                           'Audit Sample': 0,
                                                           'False Positive Rate': 0.0,
                                                           'False Negative Rate': 0.0,
                                                           'FP DPMO': 0,
                                                           'FN DPMO': 0,
                                                              'Recalls_Pts_Achieved': recalls_pts_achieved,
                                                                'Recalls_Max_Achieved': recalls_max_achieved,
                                                           'Year': year,
                                                           'Week Completed': week,
                                                           'Date Assigned': date_assigned,
                                                           'Date Completed': date_completed,
                                                           'Error Type': error_type,
                                                           'Feedback': feedback,
                                                            'Is_Biased': "NA",
                                                           'Marketplace': marketplace,
                                                           'QC Parameter Error': qc_parameter_error,
                                                           'Primary RC': primary_rc,
                                                           'Secondary RC': secondary_rc,
                                                           'Tertiary RC': tertiary_rc
                                                           })

        # print(na_latam_non_act_calculated_values.head(), na_latam_non_act_calculated_values.shape)

        return na_latam_non_act_calculated_values

    def calcs_for_na_private_brands_recalls_mw(self, NA_Private_Brands_Recalls_MW_Compiled_File_TT_Level,
                                               NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause):
        # Convert 'TT URL' column to string type
        NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause['TT URL'] = \
            NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause['TT URL'].astype(str)

        # Fill NaN values with a placeholder string
        NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause['TT URL'].fillna('NA', inplace=True)

        # Merge the DataFrames
        merged_df_na_private_brands_recalls_mw_compiled = pd.merge(
            NA_Private_Brands_Recalls_MW_Compiled_File_TT_Level,
            NA_Private_Brands_Recalls_MW_Compiled_File_Raw_Data_Errors_Root_Cause,
            how='left',
            on='TT URL'
        )


        # Reset NaN values to None
        merged_df_na_private_brands_recalls_mw_compiled['TT URL'].replace('NA', None, inplace=True)

        # select data only where year is >= 2023
        merged_df_na_private_brands_recalls_mw_compiled = merged_df_na_private_brands_recalls_mw_compiled[
            merged_df_na_private_brands_recalls_mw_compiled['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        merged_df_na_private_brands_recalls_mw_compiled = replace_commas_with_semicolon(merged_df_na_private_brands_recalls_mw_compiled)

        # Replace \n by space in dataframe
        merged_df_na_private_brands_recalls_mw_compiled = merged_df_na_private_brands_recalls_mw_compiled.replace('\n', ' ', regex=True)

        # QA Score %: Total points achieved / Total Max Points
        # Login
        # level:  (X / Y)
        qa_score_percentage_login_level = round(
            (merged_df_na_private_brands_recalls_mw_compiled['Associate Pts Achieved'] /
             merged_df_na_private_brands_recalls_mw_compiled['Associate Max Achieved']) * 100, 2).fillna(0).astype(int)

        # DPMO: (1-QA Score%)*10^6
        dpmo = round((1 - qa_score_percentage_login_level / 100) * 1000000)

        # Columns from compiled> (AD)
        tp_asins = merged_df_na_private_brands_recalls_mw_compiled['Yanked'].fillna(0).astype(int)

        # FP: column AG
        false_positive_asins = merged_df_na_private_brands_recalls_mw_compiled['Associate Level Overpulls'].fillna(0).astype(int)

        # FN: column AF
        false_negative_asins = merged_df_na_private_brands_recalls_mw_compiled['Associate Level Underpulls'].fillna(0).astype(int)

        # True Positive ASINs + False Positive ASINs (AG + AI)
        audit_sample = (tp_asins + false_positive_asins).fillna(0).astype(int)

        # False Positive Rate (Percentage of Over-pulled ASINs): False Positive ASINs / Audit Sample
        false_positive_rate = ((false_positive_asins / audit_sample) * 100).fillna(0).astype(float)

        # False Negative Rate (Percentage of Under-pulled ASINs): False Negative ASINs / ( Audit Sample - FP + FN )
        false_negative_rate = ((false_negative_asins / (
                    audit_sample - false_positive_asins + false_negative_asins)) * 100).fillna(0).astype(float)

        # FP DPMO: { 1- [ 100% - ( FP / Audit sample ) % ] } * 1000000 in percentage
        fp_dpmo = ((100 - (100 - (false_positive_asins / audit_sample))) * 1000000).fillna(0).astype(int)

        # FN DPMO: { 1- [ 100% - ( FN / Audit sample ) % ] } * 1000000 in percentage
        fn_dpmo = ((100 - (
                100 - (false_negative_asins / (audit_sample - false_positive_asins + false_negative_asins)))) * 1000000).fillna(0).astype(int)

        # -------------Deffect summary visualization ----------------

        """
        Fields and columns for the Defect Summary visual:
        Associate: column E
        TT Link: column A
        Error Type: column K
        Marketplace: column B
        QC Parameter Error: column I
        Primary RC: column L
        Secondary RC: column M
        Tertiary RC: column N
        """

        associate_na_private_brands_recalls_mw_tt_level = merged_df_na_private_brands_recalls_mw_compiled[
            'Specialist_x'].fillna("NA")
        tt_url_na_latam_non_act_tt_level = merged_df_na_private_brands_recalls_mw_compiled['TT URL'].fillna("NA")
        year = merged_df_na_private_brands_recalls_mw_compiled['Year'].fillna(pd.NaT)
        week = merged_df_na_private_brands_recalls_mw_compiled['Completed Week_x'].fillna(pd.NaT)
        date_assigned = pd.to_datetime(merged_df_na_private_brands_recalls_mw_compiled['Date Assigned']).fillna(pd.to_datetime('1900-01-01'))
        date_completed = pd.to_datetime(merged_df_na_private_brands_recalls_mw_compiled['Date Completed_x']).fillna(pd.to_datetime('1900-01-01'))
        error_type = merged_df_na_private_brands_recalls_mw_compiled['Type of Error'].fillna("NA")
        feedback = merged_df_na_private_brands_recalls_mw_compiled['Feedback'].fillna("NA")
        marketplace = merged_df_na_private_brands_recalls_mw_compiled['MP_x'].fillna("NA")
        qc_parameter_error = merged_df_na_private_brands_recalls_mw_compiled['QC Parameter Error'].fillna("NA")
        primary_rc = merged_df_na_private_brands_recalls_mw_compiled['ROOT CAUSE PRIMARY'].fillna("NA")
        secondary_rc = merged_df_na_private_brands_recalls_mw_compiled['ROOT CAUSE SECONDARY'].fillna("NA")
        tertiary_rc = merged_df_na_private_brands_recalls_mw_compiled['ROOT CAUSE TERTIARY'].fillna("NA")
        recalls_pts_achieved = merged_df_na_private_brands_recalls_mw_compiled['Recall Pts Achieved'].fillna(0).astype(int)  # Recall Pts Achieved: column CO
        recalls_max_achieved = merged_df_na_private_brands_recalls_mw_compiled['Recall Max Achieved'].fillna(0).astype(int)  # Recall Max Achieved: column CP


        # Create DataFrame

        na_private_brands_recalls_mw_compiled_calculated_values = pd.DataFrame(
            {'Source': 'NA Private Brands Recalls MW Compiled',
             'TT_URL_TT_Level': tt_url_na_latam_non_act_tt_level,
             'Specialist_TT_Level': associate_na_private_brands_recalls_mw_tt_level,
             'QA Score %': qa_score_percentage_login_level,
             'DPMO': dpmo,
             'TP ASINs': tp_asins,
             'False Positive ASINs': false_positive_asins,
             'False Negative ASINs': false_negative_asins,
             'Audit Sample': audit_sample,
             'False Positive Rate': false_positive_rate,
             'False Negative Rate': false_negative_rate,
             'FP DPMO': fp_dpmo,
             'FN DPMO': fn_dpmo,
                'Recalls_Pts_Achieved': recalls_pts_achieved,
                'Recalls_Max_Achieved': recalls_max_achieved,
             'Year': year,
             'Week Completed': week,
             'Date Assigned': date_assigned,
             'Date Completed': date_completed,
             'Error Type': error_type,
             'Feedback': feedback,
             'Is_Biased': "NA",
             'Marketplace': marketplace,
             'QC Parameter Error': qc_parameter_error,
             'Primary RC': primary_rc,
             'Secondary RC': secondary_rc,
             'Tertiary RC': tertiary_rc
             })

        return na_private_brands_recalls_mw_compiled_calculated_values

import pandas as pd
from clean_upload_data import replace_commas_with_semicolon

class QaRecallsFilesManager:

    def calcs_for_LATAM_LATAM_Recalls_Compiled_File(self, LATAM_Recalls_Compiled_File_TT_Level,
                                                    LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause):
        """
        This function calculates the following metrics for LATAM_Recalls_Compiled_File_TT_Level:
        :return:  LATAM_Recalls_Compiled_File_TT_Level
        """

        merged_df_Latam_recalls_compiled = pd.merge(
            LATAM_Recalls_Compiled_File_TT_Level,
            LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause,
            how='left',
            on='TT URL'
        )

        # select data only where year is >= 2023
        merged_df_Latam_recalls_compiled = merged_df_Latam_recalls_compiled[merged_df_Latam_recalls_compiled['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        merged_df_Latam_recalls_compiled = replace_commas_with_semicolon(merged_df_Latam_recalls_compiled)

        # Replace \n by a space in dataframe
        merged_df_Latam_recalls_compiled = merged_df_Latam_recalls_compiled.replace('\n', '', regex=True)


        # Login level: (Y / Z) * 100
        qa_score_percentage_login_level = round((merged_df_Latam_recalls_compiled['Associate Pts Achieved'] /
                                                 merged_df_Latam_recalls_compiled['Associate Max Achieved']) * 100, 2).fillna(0).astype(float)

        # DPMO: (1 - QA Score%) * 10^6
        dpmo = round((1 - qa_score_percentage_login_level / 100) * 1000000)

        # Columns from compiled> (BI)
        tp_asins = merged_df_Latam_recalls_compiled['Yanked'].fillna(0).astype(int)

        # column BK
        false_positive_asins = merged_df_Latam_recalls_compiled['Associate Level Overpulls'].fillna(0).astype(int)

        # column BJ
        false_negative_asins = merged_df_Latam_recalls_compiled['Associate Level Underpulls'].fillna(0).astype(int)

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
        specialist_tt_level = merged_df_Latam_recalls_compiled['Specialist_x'].fillna("NA")  # Associate: column M
        # specialist_raw_data_errors = LATAM_Recalls_Compiled_File_Raw_Data_Errors_Root_Cause['Specialist'] # Raw Data Errors: column E
        year = merged_df_Latam_recalls_compiled['Year'].fillna(pd.NaT)  # Year: column B
        marketplace_tt_level = merged_df_Latam_recalls_compiled['MP_x'].fillna("NA")  # Marketplace: column I
        week = merged_df_Latam_recalls_compiled['Completed Week_x'].fillna(pd.NaT)  # Week: column D
        date_assigned = pd.to_datetime(merged_df_Latam_recalls_compiled['Date Assigned']).fillna(pd.to_datetime('1900-01-01'))  # Date Assigned: column F
        date_completed = pd.to_datetime(
            merged_df_Latam_recalls_compiled['Date Completed_x']).fillna(pd.to_datetime('1900-01-01'))  # Date Completed: column G
        tt_URL_tt_level = merged_df_Latam_recalls_compiled['TT URL'].fillna("NA")  # TT Link: column K
        error_type = merged_df_Latam_recalls_compiled['Type of Error'].fillna("NA")  # Error Type: column CI
        feedback = merged_df_Latam_recalls_compiled['Feedback'].fillna("NA")  # Feedback: column CJ
        is_biased = merged_df_Latam_recalls_compiled['Unbiased/Biased_x'].fillna("NA")  # Is Biased: column E
        qc_parameter_error = merged_df_Latam_recalls_compiled['QC Parameter Error'].fillna("NA")  # QC Parameter Error: column CG
        primary_rc = merged_df_Latam_recalls_compiled['ROOT CAUSE PRIMARY'].fillna("NA")  # Primary RC: column CJ
        secondary_rc = merged_df_Latam_recalls_compiled['ROOT CAUSE SECONDARY'].fillna("NA")  # Secondary RC: column CK
        tertiary_rc = merged_df_Latam_recalls_compiled['ROOT CAUSE TERTIARY'].fillna("NA")  # Tertiary RC: column CL
        recalls_pts_achieved = merged_df_Latam_recalls_compiled['Recall Pts Achieved'].fillna(0).astype(int)  # Recall Pts Achieved: column CO
        recalls_max_achieved = merged_df_Latam_recalls_compiled['Recall Max Achieved'].fillna(0).astype(int)  # Recall Max Achieved: column CP

        # Create dataframe
        latam_recalls_compiled_calculated_values = pd.DataFrame({'Source': 'LATAM_Recalls_Compiled_File',
                                                                 'TT_URL_TT_Level': tt_URL_tt_level,
                                                                 'Specialist_TT_Level': specialist_tt_level,
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
                                                                'Is_Biased': is_biased,
                                                                 'Marketplace': marketplace_tt_level,
                                                                 'QC Parameter Error': qc_parameter_error,
                                                                 'Primary RC': primary_rc,
                                                                 'Secondary RC': secondary_rc,
                                                                 'Tertiary RC': tertiary_rc,
                                                                    })
        return latam_recalls_compiled_calculated_values

    def calcs_for_MW_Compiled_File(self, MW_Compiled_File_MW_Data, MW_Compiled_File_Raw_Data_Errors_Root_Cause):
        """
        This function calculates the following metrics for MW_Compiled_File_MW_Data:
        :return: MW_Compiled_File_MW_Data
        """

        merged_df_mw_compiled = pd.merge(
            MW_Compiled_File_MW_Data,
            MW_Compiled_File_Raw_Data_Errors_Root_Cause,
            how='left',
            on='TT URL'
        )

        # select data only where year is >= 2023
        merged_df_mw_compiled = merged_df_mw_compiled[merged_df_mw_compiled['Year'] >= 2023]



        # Replace commas by semmincolons in dataframe
        merged_df_mw_compiled = replace_commas_with_semicolon(merged_df_mw_compiled)

        # Replace \n by a space in dataframe
        merged_df_mw_compiled = merged_df_mw_compiled.replace('\n', ' ', regex=True)

        # QA Score %: Total points achieved / Total Max Points
        # Login
        # level: (N / O)
        qa_score_percentage_login_level = round((merged_df_mw_compiled['Associate Pts Achieved'] /
                                                 merged_df_mw_compiled['Associate Max Achieved']) * 100, 2).fillna(0).astype(float)

        # DPMO: (1-QA Score%)*10^6
        dpmo = round((1 - qa_score_percentage_login_level / 100) * 1000000)

        # Columns from compiled> (AJ)
        tp_asins = merged_df_mw_compiled['Yanked'].fillna(0).astype(int)

        # FP: column AL
        false_positive_asins = merged_df_mw_compiled['Associate Level Overpulls'].fillna(0).astype(int)

        # FN: column AK
        false_negative_asins = merged_df_mw_compiled['Associate Level Underpulls'].fillna(0).astype(int)

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
        tt_url_mw_data = merged_df_mw_compiled['TT URL'].fillna("NA")
        speacialist_mw_data = merged_df_mw_compiled['Specialist_x'].fillna("NA")
        year = merged_df_mw_compiled['Year'].fillna(pd.NaT)
        week = merged_df_mw_compiled['Completed Week_x'].fillna(pd.NaT)
        date_assigned = pd.to_datetime(merged_df_mw_compiled['Date Assigned']).fillna(pd.to_datetime('1900-01-01'))
        date_completed = pd.to_datetime(merged_df_mw_compiled['Date Completed_x']).fillna(pd.to_datetime('1900-01-01'))
        error_type = merged_df_mw_compiled['Type of Error'].fillna("NA")
        feedback = merged_df_mw_compiled['Feedback'].fillna("NA")
        marketplace = merged_df_mw_compiled['MP_x'].fillna("NA")
        qc_parameter_error = merged_df_mw_compiled['QC Parameter Error'].fillna("NA")
        primary_rc = merged_df_mw_compiled['ROOT CAUSE PRIMARY'].fillna("NA")
        # secondary_rc = MW_Compiled_File_Raw_Data_Errors_Root_Cause['ROOT CAUSE SECONDARY']  # These two cols do not exist in file
        # tertiary_rc = MW_Compiled_File_Raw_Data_Errors_Root_Cause['ROOT CAUSE TERTIARY']
        recalls_pts_achieved = merged_df_mw_compiled['MW Pts Achieved'].fillna(0).astype(int)  # Recall Pts Achieved: column CO
        recalls_max_achieved = merged_df_mw_compiled['MW Max Achieved'].fillna(0).astype(int)  # Recall Max Achieved: column CP

        # Create dataframe
        mw_compiled_calculated_values = pd.DataFrame({'Source': 'MW_Compiled_File',
                                                      'TT_URL_TT_Level': tt_url_mw_data,
                                                      'Specialist_TT_Level': speacialist_mw_data,
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
                                                      'Secondary RC': "NA",
                                                      'Tertiary RC': "NA"
                                                      })

        # print(mw_compiled_calculated_values.head(), mw_compiled_calculated_values.shape)
        return mw_compiled_calculated_values

    def calcs_for_global_latam(self, Global_Recalls_Compiled_File_LATAM_TT_Level,  Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause):
        """
        This function calculates the following metrics for LATAM_Recalls_Compiled_File_MW_Data:
        :return: Global_Recalls_Compiled_File_LATAM_Calculated_Values
        """

        merged_df_global_latam = pd.merge(
            Global_Recalls_Compiled_File_LATAM_TT_Level,
            Global_Recalls_Compiled_File_LATAM_Raw_Data_Errors_Root_Cause,
            how='left',
            on='TT URL',
        )

        # select data only where year is >= 2023
        merged_df_global_latam = merged_df_global_latam[merged_df_global_latam['Year'] >= 2023]

        # Replace commas by semmincolons in dataframe
        merged_df_global_latam = replace_commas_with_semicolon(merged_df_global_latam)

        # Replace \n by a space in dataframe
        merged_df_global_latam = merged_df_global_latam.replace(r'\n', ' ', regex=True)

        # QA Score %: Total points achieved / Total Max Points
        # Login
        # level: (AA / AB)
        qa_score_percentage_login_level = round((merged_df_global_latam['Associate Pts Achieved'] /
                                                 merged_df_global_latam['Associate Max Achieved']) * 100, 2).fillna(0).astype(int)

        # DPMO: (1-QA Score%)*10^6
        dpmo = round((1 - qa_score_percentage_login_level / 100) * 1000000)

        # Columns from compiled> (AG)
        tp_asins = merged_df_global_latam['Yanked'].fillna(0).astype(int)

        # FP: column AI
        false_positive_asins = merged_df_global_latam['Associate Level Overpulls'].fillna(0).astype(int)

        # FN: column AH
        false_negative_asins = merged_df_global_latam['Associate Level Underpulls'].fillna(0).astype(int)

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

        tt_url_global_latam_tt_url = merged_df_global_latam['TT URL'].fillna("NA")
        specialist_global_latam_tt_url = merged_df_global_latam['Specialist_x'].fillna("NA")
        year = merged_df_global_latam['Year'].fillna(pd.NaT)
        week = merged_df_global_latam['Completed Week_x'].fillna(pd.NaT)
        date_assigned = pd.to_datetime(merged_df_global_latam['Date Assigned']).fillna(pd.to_datetime('1900-01-01'))
        date_completed = pd.to_datetime(merged_df_global_latam['Date Completed_x']).fillna(pd.to_datetime('1900-01-01'))
        error_type = merged_df_global_latam['Type of Error'].fillna("NA")
        feedback = merged_df_global_latam['Feedback'].fillna("NA")
        is_biased = merged_df_global_latam['Unbiased/Biased_x'].fillna("NA")
        marketplace = merged_df_global_latam['MP_x'].fillna("NA")
        qc_parameter_error = merged_df_global_latam['QC Parameter Error'].fillna("NA")
        primary_rc = merged_df_global_latam['ROOT CAUSE PRIMARY'].fillna("NA")
        secondary_rc = merged_df_global_latam['ROOT CAUSE SECONDARY'].fillna("NA")  # These two cols do not exist in file
        tertiary_rc = merged_df_global_latam['ROOT CAUSE TERTIARY'].fillna("NA")
        recalls_pts_achieved = merged_df_global_latam['Recall Pts Achieved'].fillna(0).astype(int)  # Recall Pts Achieved: column CO
        recalls_max_achieved = merged_df_global_latam['Recall Max Achieved'].fillna(0).astype(int)  # Recall Max Achieved: column CP


        # Create dataframe

        global_latam_calculated_values = pd.DataFrame({'Source': 'Global_Recalls_Compiled_File',
                                                       'TT_URL_TT_Level': tt_url_global_latam_tt_url,
                                                       'Specialist_TT_Level': specialist_global_latam_tt_url,
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
                                                         'Is_Biased': is_biased,
                                                       'Marketplace': marketplace,
                                                       'QC Parameter Error': qc_parameter_error,
                                                       'Primary RC': primary_rc,
                                                       'Secondary RC': secondary_rc,
                                                       'Tertiary RC': tertiary_rc
                                                       })

        return global_latam_calculated_values

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


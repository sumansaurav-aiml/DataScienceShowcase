# import the module
from sqlalchemy import create_engine
import sqlalchemy as sa
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plot
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages
import logging

def ExportAnalysisData(jobid,conn_string):
    logging.basicConfig(level=logging.INFO, filename='./data/jobid'+str(jobid)+'/JobId'+jobid+'.log', filemode='a', format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger("ExportAnalysisData")
    try:
        # create sqlalchemy engine
        engine = create_engine(conn_string)
        with engine.connect() as conn:    
            logger.info("You're connected!")
            logger.info("Fetching Job Run Data!")
            data = conn.execute("""SELECT
                            AIML_JOB_RUN_ID, to_char (START_DATE_TIME,'dd-mm-yy hh24:mi:ss') START_DATE_TIME, to_char(NEXT_START_DATE_TIME,'dd-mm-yy hh24:mi:ss') NEXT_START_DATE_TIME
                        FROM
                            (
                                SELECT
                                    aiml_job_run_id,
                                    start_date_time,
                                    LEAD(start_date_time, 1) OVER(
                                        ORDER BY
                                            start_date_time
                                    ) AS next_start_date_time,
                                    status
                                FROM
                                    aiml_job_run_status
                                WHERE
                                    aiml_job_id = (
                                        SELECT
                                            aiml_job_id
                                        FROM
                                            aiml_jobs
                                        WHERE
                                            aiml_job_name = 'RecommendationForIngram'
                                    )
                            )
                        WHERE
                            status = 'Completed'""")
            all_rows=data.fetchall()
            try:
                jobrundf=pd.DataFrame(list(all_rows),columns=["AIML_JOB_RUN_ID","START_DATE_TIME","NEXT_START_DATE_TIME"])
                logger.info("ANALYSIS DATA Dataframe got generated with number of records: {}".format(jobrundf.shape[0]))
                try:
                    # remove the timedelta part.
                    jobrundf['START_DATE_TIME'] =  pd.to_datetime(jobrundf['START_DATE_TIME'], format='%d-%m-%y %H:%M:%S')-timedelta(days=20)
                    jobrundf['NEXT_START_DATE_TIME'] =  pd.to_datetime(jobrundf['NEXT_START_DATE_TIME'], format='%d-%m-%y %H:%M:%S')-timedelta(days=20)
                    # calculate precision and recall at k
                    # tp/tp+fp - total predicted +ve
                    def precision(actual, predicted, top_n):
                        act_set = set(actual)
                        pred_set = set(predicted[:top_n])
                        result = len(act_set & pred_set) / float(top_n)
                        return result

                    # tp/tp+fn - total actual +ve
                    def recall(actual, predicted, top_n):
                        act_set = set(actual)
                        pred_set = set(predicted[:top_n])
                        result = len(act_set & pred_set) / float(len(act_set))
                        return result
                    
                    ##########################################################################################
                    try:
                        #finding the last run jobid
                        maxjobid = jobrundf.AIML_JOB_RUN_ID.max()
                        logger.info("Last executed jobid: {}".format(str(maxjobid)))
                        minjobid = jobrundf.AIML_JOB_RUN_ID.min()
                        logger.info("Min job id {}".format(str(minjobid)))
                        pdfname = './data/jobid'+str(maxjobid)+'/predictionfiles/Ingram_RecSys_Eval_'+str(jobrundf[jobrundf['AIML_JOB_RUN_ID']==maxjobid].iat[0,1].date())+'.pdf'
                        
                        #validate last 6 months data only hence find a 6 months old date from latest job run date
                        six_month_old_date = jobrundf[jobrundf['AIML_JOB_RUN_ID']==maxjobid].iat[0,1]-timedelta(days=180)

                        perjobactivity = dict() #dict to keep activity data of each job
                        perjobrec = dict() #dict to keep hybrid rec for each job
                        perjobtrend = dict() #dict to keep trending item rec for each job

                        #check if it next run date for the last job is null, it should be null
                        if pd.isna(jobrundf[jobrundf['AIML_JOB_RUN_ID']==maxjobid].iat[0,2]):
                            #since the last executed job has the most recent activity data hence just importing it
                            try:
                                latestactivitydf = pd.read_csv('./data/jobid'+str(maxjobid)+'/trainingfiles/USER_ACTIVITY_WITH_TIME_PORTAL.CSV')
                            except Exception as error:
                                logger.error("Exception occurred while reading USER_ACTIVITY_WITH_TIME_PORTAL for latest job")
                                raise

                            #below part is same as in the original RecommenderSystemForIngramHybridModel.py file
                            latestactivitydf.dropna(subset=['PORTALNAME'],inplace=True)

                            #Since Download, Pull, Video have are essentially same action, hence combining them
                            def combining_activation_download_actions(action):
                                if action=='ACTIVATECAMPAIGN':
                                    return "ACTIVATECAMPAIGN"
                                elif action == 'DOWNLOADCAMPAIGN':
                                    return "ACTIVATECAMPAIGN"        
                                else:
                                    return action
                            # Generalizing the activation_download_actions action.
                            latestactivitydf['ACTIVITY_TYPE'] = latestactivitydf.apply(lambda x: combining_activation_download_actions(x['ACTIVITY_TYPE']),axis=1)
                            #Since we have PDF downloads appended with tactics id, lets generalize them as "Download" action
                            def generalizing_actions(action):
                                if action.find("DOWNLOAD")>=0:
                                    return "DOWNLOAD"
                                else:
                                    return action
                            # Generalizing the pdf download action.
                            latestactivitydf['ACTIVITY_TYPE'] = latestactivitydf.apply(lambda x: generalizing_actions(x['ACTIVITY_TYPE']),axis=1)
                            #As we can see that "EnterCampaignStartmarketing" has been done only once, this seems to be a wrong data, lets remove it.
                            latestactivitydf = latestactivitydf.loc[latestactivitydf['ACTIVITY_TYPE']!='ENTERCAMPAIGNSTARTMARKETING']
                            #Also lets remove the "Close" actions 
                            latestactivitydf = latestactivitydf.loc[latestactivitydf['ACTIVITY_TYPE']!='CLOSEASSETPREVIEW']
                            latestactivitydf = latestactivitydf.loc[latestactivitydf['ACTIVITY_TYPE']!='CLOSESETUPASSETS']
                            #Since Download, Pull, Video have are essentially same action, hence combining them
                            def combining_pull_actions(action):
                                if action=='VIDEO_GETDEFAULTEMBEDCODE':
                                    return "PULL"
                                elif action == 'DOWNLOAD':
                                    return "PULL"        
                                else:
                                    return action
                            # Combining Download, Pull, Video.
                            latestactivitydf['ACTIVITY_TYPE'] = latestactivitydf.apply(lambda x: combining_pull_actions(x['ACTIVITY_TYPE']),axis=1)
                            #Similarly EnterCampaignOverview and ReturnCampaignOverivew and EnterCampaignsTab are same kind of action, hence combining them
                            def combining_enter_camp_actions(action):
                                if action=='ENTERCAMPAIGNSTAB':
                                    return "ENTERCAMPAIGNOVERVIEW"
                                elif action == 'RETURNCAMPAIGNOVERVIEW':
                                    return "ENTERCAMPAIGNOVERVIEW"   
                                elif action == 'ENTERPRODUCT':
                                    return "ENTERCAMPAIGNOVERVIEW"
                                else:
                                    return action

                            # Combining Download, Pull, Video.
                            latestactivitydf['ACTIVITY_TYPE'] = latestactivitydf.apply(lambda x: combining_enter_camp_actions(x['ACTIVITY_TYPE']),axis=1)

                            #these are the unique activities performed by the users
                            uniqueactivities = latestactivitydf.ACTIVITY_TYPE.unique().tolist()

                            #converting timestamp column to datetime type
                            latestactivitydf['TIMESTAMP'] =  pd.to_datetime(latestactivitydf['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')

                            #if jobid is not the last executed job, fill the three dictionaries based on duration of each recommendation for respective folder
                            for index, row in jobrundf.iterrows():
                                if row.AIML_JOB_RUN_ID!=maxjobid:
                                    if row.AIML_JOB_RUN_ID == minjobid:
                                        pastactivities = latestactivitydf[latestactivitydf['TIMESTAMP']<row.START_DATE_TIME]
                                    if row.START_DATE_TIME>=six_month_old_date:
                                        try:
                                            perjobrec[row.AIML_JOB_RUN_ID] = pd.read_csv('./data/jobid'+str(row.AIML_JOB_RUN_ID)+'/predictionfiles/TOP_HYBRID_REC_FOR_PORTALS_USERS.CSV')
                                            logger.info("Fetching TOP_HYBRID_REC_FOR_PORTALS_USERS data for job: {}".format(str(row.AIML_JOB_RUN_ID)))
                                            perjobtrend[row.AIML_JOB_RUN_ID] = pd.read_csv('./data/jobid'+str(row.AIML_JOB_RUN_ID)+'/predictionfiles/TOP_TRENDING_PROD_FOR_PORTALS.CSV')  
                                            logger.info("Fetching TOP_TRENDING_PROD_FOR_PORTALS data for job: {}".format(str(row.AIML_JOB_RUN_ID)))
                                            perjobactivity[row.AIML_JOB_RUN_ID] = latestactivitydf[(latestactivitydf['TIMESTAMP']>row.START_DATE_TIME) & (latestactivitydf['TIMESTAMP']<=row.NEXT_START_DATE_TIME)]
                                        except Exception as error:
                                            logger.error("Exception occurred while reading data for TOP_HYBRID_REC_FOR_PORTALS_USERS for jobid: {}".format(str(row.AIML_JOB_RUN_ID)), exc_info=True)
                                            #raise                        
                        else:
                            logger.error("Last job should not have a valid NEXT_START_DATE_TIME")
                            raise Exception("Last job should not have a valid NEXT_START_DATE_TIME")
                    except Exception as error:
                        logger.error("Exception occurred while reading the CSVs from path")
                        raise
                        
                        
                    # hardcoded recommendation being shown to US portal users
                    us_portal = ['NOMADESK BUSINESS FILE SHARING & SYNCHRONIZATION',
                    'UMBRELLA',
                    'PICKIT BUSINESS',
                    'MICROSOFT ENTERPRISE MOBILITY + SECURITY',
                    'DROPBOX BUSINESS',
                    'INTERMEDIA OFFICE IN THE CLOUD',
                    'ACRONIS CYBER CLOUD',
                    'AUTODESK SOFTWARE COLLECTIONS',
                    'ITSMINE BEYOND DATA LOSS PREVENTION (DLP)',
                    'NYOTRON PARANOID',
                    'MICROSOFT DYNAMICS 365',
                    'IBM CLOUD',
                    'WEBEX',
                    'LETSIGNIT',
                    'AMAZON WEB SERVICES',
                    'IBM SPECTRUM PROTECT PLUS',
                    'SERVICEAIDE LUMA VIRTUAL AGENT',
                    'SYMANTEC ENDPOINT PROTECTION MOBILE',
                    'CORENT SURPAAS MAAS',
                    'IBM SPSS STATISTICS',
                    'TREND MICRO WORRY-FREE SERVICES',
                    'PIXM ANTI-PHISHING',
                    'AVEPOINT MIGRATION',
                    'ECWID',
                    'RSIGN',
                    'MOVERE DISCOVERY',
                    'MICROSOFT AZURE',
                    'CLOUDAMIZE',
                    'CONNECTWISE',
                    'MICROSOFT WINDOWS 10 ENTERPRISE',
                    'IBM STORAGE INSIGHTS PRO',
                    'INTUIT QUICKBOOKS ONLINE',
                    'STORAGE GUARDIAN BACKUP AND DR SOLUTIONS',
                    'AVEPOINT CLOUD BACKUP',
                    'NEW DAY AT WORK WORKSPACE 365',
                    'VEEAM BACKUP & REPLICATION',
                    'ACRONIS CYBER BACKUP CLOUD',
                    'MSPCOMPLETE',
                    'DROPSUITE EMAIL BACKUP AND ARCHIVING',
                    'SYMANTEC EMAIL SECURITY.CLOUD',
                    'VAULT AMERICA CLOUD BACKUP & RECOVERY',
                    'IBM SPSS MODELER',
                    'WITTYPARROT',
                    'LAWTOOLBOX365',
                    'SYMANTEC CLOUD WORKLOAD PROTECTION',
                    'EVERNOTE',
                    'TREND MICRO HOSTED EMAIL SECURITY',
                    'ESET SECURITY SOLUTIONS',
                    'YOLA SITEBUILDER',
                    'ENTRUST DATACARD',
                    'MICROSOFT 365',
                    'IBM SPECTRUM SCALE',
                    'IBM MAAS360 WITH WATSON',
                    'KASPERSKY ENDPOINT PROTECTION / SECURITY',
                    'INGRAM MICRO COST OPTIMIZATION SERVICES',
                    '8X8 X SERIES',
                    'APC BY SCHNEIDER ELECTRIC - ECOSTRUXURE IT EXPERT',
                    'SANEBOX',
                    'IDSYNC IDENTITY SYNCHRONIZER',
                    'SYMANTEC ENDPOINT PROTECTION CLOUD',
                    'BITTITAN MIGRATIONWIZ',
                    'RPOST RMAIL',
                    'DOCUSIGN BUSINESS PRO CLOUD EDITION',
                    'DELIVERYSLIP',
                    'APPTIVO',
                    'DROPMYSITE WEBSITE BACKUP',
                    'GOOGLE G SUITE',
                    'MICROSOFT OFFICE 365',
                    'NERDIO FOR AZURE',
                    'TREND MICRO CLOUD APP SECURITY',
                    'NUANCE POWER PDF ADVANCED',
                    'RPOST RMAIL']   
                    
                    #declaring a Dataframe which will have all the required column to show the stats
                    list1 = ["AIML_JOB_RUN_ID", "ACTIVITY_DATE", "USER_ID", "IS_NEW_USER", "IS_TEST_USER", "PORTAL", "PRECISION@5", "RECALL@5", "PRECISION@10", "RECALL@10", "PRECISION@20", "RECALL@20"]
                    columns = list1 + uniqueactivities
                    final_analysis_df = pd.DataFrame(columns=columns)

                    #filling dataframe for each jobid
                    for j in perjobactivity.keys():
                        for user in perjobactivity[j].USER_ID.unique().tolist():
                            isnewuser = 0 # logic for consideration of a new user is that from the latest activity data we look for the users whose activity
                            #got added during a certain week after the job run, so if the data is there in the activity dataframe but not in the model dataframe
                            #then it is considered as a new user. Note:User who got filtered in the activity data query, those user will be treated as new user in the UI integration.

                            istestuser = perjobactivity[j][perjobactivity[j]['USER_ID']==user][:1].iat[0,6]
                            portal = perjobactivity[j][perjobactivity[j]['USER_ID']==user][:1].iat[0,4]
                            relprod = perjobactivity[j][perjobactivity[j]['USER_ID']==user].ITEM_ID.unique().tolist()

                            if istestuser == 0:
                                #if there is no data in hybrid rec then consider him as a new user
                                if len(perjobrec[j][perjobrec[j]['USER_ID']==user].ITEM_ID.tolist())==0:
                                    isnewuser = 1
                                if portal == 'INGRAM US HUB':
                                    recprod = us_portal.copy()
                                else:
                                    recprod = []
                            else:
                                recprod = perjobrec[j][perjobrec[j]['USER_ID']==user].ITEM_ID.tolist()
                                #if there is no data in hybrid rec then consider him as a new user
                                if len(recprod)==0:        
                                    recprod = perjobtrend[j][perjobtrend[j]['PORTAL']==portal].ITEM_ID.tolist()
                                    isnewuser = 1

                            #getting the max time of acitivty, this will help us to compare month by month stats
                            activityperiod = perjobactivity[j][perjobactivity[j]['USER_ID']==user].TIMESTAMP.max()

                            #print('user:', user, 'relprod:', relprod, 'recprod:', recprod)        
                            new_row = {'AIML_JOB_RUN_ID':j,
                                       'ACTIVITY_DATE':activityperiod,
                                       'USER_ID':user,
                                       'IS_NEW_USER':isnewuser,
                                       'IS_TEST_USER':istestuser,
                                       'PORTAL':portal,               
                                       'PRECISION@5':precision(relprod, recprod, top_n = 5),
                                       'RECALL@5':recall(relprod, recprod, top_n = 5),
                                       'PRECISION@10':precision(relprod, recprod, top_n = 10),
                                       'RECALL@10':recall(relprod, recprod, top_n = 10),
                                       'PRECISION@20':precision(relprod, recprod, top_n = 20),
                                       'RECALL@20':recall(relprod, recprod, top_n = 20)}
                            activitycnt = perjobactivity[j][perjobactivity[j]['USER_ID']==user][['ACTIVITY_TYPE','ITEM_ID']].groupby(['ACTIVITY_TYPE'], as_index=False).count()
                            for i in uniqueactivities:
                                if activitycnt[activitycnt['ACTIVITY_TYPE']==i].shape[0]>0:
                                    new_row = {**new_row, **{i:activitycnt[activitycnt['ACTIVITY_TYPE']==i].iat[0,1]}}
                            final_analysis_df = final_analysis_df.append(new_row,ignore_index=True)

                    #adding past data in the df
                    for user in pastactivities.USER_ID.unique().tolist():
                        istestuser = pastactivities[pastactivities['USER_ID']==user][:1].iat[0,6]
                        portal = pastactivities[pastactivities['USER_ID']==user][:1].iat[0,4]
                        #print('user:', user, 'relprod:', relprod, 'recprod:', recprod)        
                        new_row = {'AIML_JOB_RUN_ID':0,
                                   #'ACTIVITY_DATE':activityperiod,
                                   'USER_ID':user,
                                   #'IS_NEW_USER':isnewuser,
                                   'IS_TEST_USER':istestuser,
                                   'PORTAL':portal,               
                                   #'PRECISION@5':precision(relprod, recprod, top_n = 5),
                                   #'RECALL@5':recall(relprod, recprod, top_n = 5),
                                   #'PRECISION@10':precision(relprod, recprod, top_n = 10),
                                   #'RECALL@10':recall(relprod, recprod, top_n = 10),
                                   #'PRECISION@20':precision(relprod, recprod, top_n = 20),
                                   #'RECALL@20':recall(relprod, recprod, top_n = 20)
                                  }
                        activitycnt = pastactivities[pastactivities['USER_ID']==user][['ACTIVITY_TYPE','ITEM_ID']].groupby(['ACTIVITY_TYPE'], as_index=False).count()
                        for i in uniqueactivities:
                            if activitycnt[activitycnt['ACTIVITY_TYPE']==i].shape[0]>0:
                                new_row = {**new_row, **{i:activitycnt[activitycnt['ACTIVITY_TYPE']==i].iat[0,1]}}
                        final_analysis_df = final_analysis_df.append(new_row,ignore_index=True)

                    logger.info("Exporting ANALYSIS_DATA to CSV file ANALYSIS_DATA")
                    final_analysis_df.to_csv('./data/jobid'+str(jobid)+'/predictionfiles/ANALYSIS_DATA.CSV',index=False)
                    logger.info("Finished Exporting ANALYSIS_DATA!")

                    ##############################################################
                    #Exporting the Dataframe to DB so that Tableau reports can be created on top of it
                    try:
                        if final_analysis_df.shape[0] > 0:    
                            #modify the column as per table
                            final_analysis_df["AIML_JOB_RUN_ID"] = pd.to_numeric(final_analysis_df["AIML_JOB_RUN_ID"])    
                            final_analysis_df["USER_ID"] = pd.to_numeric(final_analysis_df["USER_ID"])    
                            final_analysis_df["IS_NEW_USER"] = pd.to_numeric(final_analysis_df["IS_NEW_USER"])
                            final_analysis_df["IS_TEST_USER"] = pd.to_numeric(final_analysis_df["IS_TEST_USER"])
                            final_analysis_df["PRECISION@5"] = pd.to_numeric(final_analysis_df["PRECISION@5"])
                            final_analysis_df["RECALL@5"] = pd.to_numeric(final_analysis_df["RECALL@5"])
                            final_analysis_df["PRECISION@10"] = pd.to_numeric(final_analysis_df["PRECISION@10"])
                            final_analysis_df["RECALL@10"] = pd.to_numeric(final_analysis_df["RECALL@10"])
                            final_analysis_df["PRECISION@20"] = pd.to_numeric(final_analysis_df["PRECISION@20"])
                            final_analysis_df["RECALL@20"] = pd.to_numeric(final_analysis_df["RECALL@20"])
                            final_analysis_df["ENTERCAMPAIGNOVERVIEW"] = pd.to_numeric(final_analysis_df["ENTERCAMPAIGNOVERVIEW"])
                            final_analysis_df["PULL"] = pd.to_numeric(final_analysis_df["PULL"])
                            final_analysis_df["OPENASSETPREVIEW"] = pd.to_numeric(final_analysis_df["OPENASSETPREVIEW"])
                            final_analysis_df["ACTIVATECAMPAIGN"] = pd.to_numeric(final_analysis_df["ACTIVATECAMPAIGN"])
                            final_analysis_df["CAMPAIGNSTATUS"] = pd.to_numeric(final_analysis_df["CAMPAIGNSTATUS"])
                            final_analysis_df["SETUPASSETS"] = pd.to_numeric(final_analysis_df["SETUPASSETS"])
                            object_columns = [c for c in final_analysis_df.columns[final_analysis_df.dtypes == 'object'].tolist()]
                            dtyp = {c:sa.types.VARCHAR(final_analysis_df[c].str.len().max()) for c in object_columns}
                            try:
                                # create sqlalchemy engine
                                engine = create_engine(conn_string)
                                with engine.connect() as conn:
                                    logger.info("You're connected!")
                                    try:
                                        #print("Transaction begins!")
                                        logger.info("Transaction begins!")
                                        tran = conn.begin()
                                        logger.info("Truncating table REC_SYSTEM_ANALYSIS_DATA!")
                                        data = conn.execute("""TRUNCATE TABLE REC_SYSTEM_ANALYSIS_DATA""")                        
                                        logger.info("Inserting data into REC_SYSTEM_ANALYSIS_DATA!")
                                        final_analysis_df.to_sql('REC_SYSTEM_ANALYSIS_DATA', con = engine, if_exists = 'append', chunksize = 1000,index=False, dtype=dtyp) # takes 45 sec for 100000 records
                                        logger.info("Rows inserted successfully: {}".format(final_analysis_df.shape[0]))                        
                                        tran.commit()
                                    except Exception as error:
                                        logger.error("Error occurred, rolling back!")
                                        tran.rollback()
                                        raise
                                    finally:
                                        #print("Closing Connection!")
                                        logger.info("Closing Connection!")
                                        conn.close()
                            except Exception as error: 
                                raise
                            else:
                                #print("Data exported successfully!!!")
                                logger.info("Data exported successfully!!!")
                        else:
                            raise Exception("Data not found in the csv file: TOP_COL_FILTER_REC_PROD_FOR_PORTALS_USERS.CSV")
                    except Exception as error:
                        #print("Exception occurred: ", error)
                        logger.error("Exception occurred", exc_info=True)
                        raise
                    ##############################################################


                    with PdfPages(pdfname) as pdf:
                        ##############################################################
                        #getting US portal data for test and non-test users in two different df grouped by jobid
                        fad_us_1_perjob = final_analysis_df[(final_analysis_df['PORTAL']=='INGRAM US HUB') & (final_analysis_df['IS_TEST_USER']==1) & (final_analysis_df['AIML_JOB_RUN_ID']!=0)].groupby(['AIML_JOB_RUN_ID']).agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_us_0_perjob = final_analysis_df[(final_analysis_df['PORTAL']=='INGRAM US HUB') & (final_analysis_df['IS_TEST_USER']==0) & (final_analysis_df['AIML_JOB_RUN_ID']!=0)].groupby(['AIML_JOB_RUN_ID']).agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        ##############################################################
                        # Please note the range of the two plots could be different
                        # Comparision of precision and recall for each job
                        figure, axes = plot.subplots(2, 1, figsize=(30, 15))
                        fad_us_1_perjob[['PRECISION@5','RECALL@5','PRECISION@10','RECALL@10','PRECISION@20','RECALL@20']].plot.bar(ax=axes[0],title='Precision and recall for Test Users - US Portal')
                        plot.legend(loc='upper right')
                        fad_us_0_perjob[['PRECISION@5','RECALL@5','PRECISION@10','RECALL@10','PRECISION@20','RECALL@20']].plot.bar(ax=axes[1],title='Precision and recall for Non-test Users - US Portal')
                        plot.legend(loc='upper right')
                        plot.text(-0.50,0.205,"Stats for US portal between test and non-test users", fontsize=20, color='red')
                        pdf.savefig()
                        plot.close()    
                        ##############################################################
                        # Please note the range of the two plots could be different
                        # Comparision of total activities for each job 
                        figure, axes = plot.subplots(2, 1, figsize=(30, 15))
                        fad_us_1_perjob[uniqueactivities].plot.bar(ax=axes[0],title='Activities count for Test Users')
                        plot.legend(loc='upper right')    
                        fad_us_0_perjob[uniqueactivities].plot.bar(ax=axes[1],title='Activities count for Non-test Users')
                        plot.legend(loc='upper right')
                        plot.text(-0.50,150,"Stats for US portal between test and non-test users", fontsize=20, color='red')
                        pdf.savefig()
                        plot.close()
                        ##############################################################
                        # Comparision of precision and recall and all activities overall
                        fad_us_1_series = fad_us_1_perjob.agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_us_0_series = fad_us_0_perjob.agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_us_1 = pd.DataFrame(fad_us_1_series)
                        fad_us_0 = pd.DataFrame(fad_us_0_series)

                        fad_us_merged = pd.merge(fad_us_1, fad_us_0, left_index=True, right_index=True)
                        fad_us_merged.rename(columns = {'0_x':'TestUser', '0_y':'Non-testUser'}, inplace = True)
                        
                        figure, axes = plot.subplots(2, 1, figsize=(30, 15))
                        
                        xx = fad_us_merged[:6].plot.bar(ax=axes[0],title='Overall precision and recall - US Portal')
                        xx.set_xticklabels( fad_us_merged[:6].index.tolist(),rotation=0)
                        yy = fad_us_merged[6:].plot.bar(ax=axes[1],title='Overall count of activities - US Portal')
                        yy.set_xticklabels( fad_us_merged[6:].index.tolist(),rotation=0)
                        plot.text(-0.50,575,"Stats for US portal between test and non-test users", fontsize=20, color='red')
                        pdf.savefig()
                        plot.close()
                        ##############################################################
                        fad_us_1_perjob_newuser = final_analysis_df[(final_analysis_df['PORTAL']=='INGRAM US HUB') & (final_analysis_df['IS_TEST_USER']==1) & (final_analysis_df['IS_NEW_USER']==1) & (final_analysis_df['AIML_JOB_RUN_ID']!=0)].groupby(['AIML_JOB_RUN_ID']).agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_us_0_perjob_newuser = final_analysis_df[(final_analysis_df['PORTAL']=='INGRAM US HUB') & (final_analysis_df['IS_TEST_USER']==0) & (final_analysis_df['IS_NEW_USER']==1) & (final_analysis_df['AIML_JOB_RUN_ID']!=0)].groupby(['AIML_JOB_RUN_ID']).agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        ##############################################################
                        # Please note the range of the two plots could be different
                        # Comparision of precision and recall for each job for new users
                        figure, axes = plot.subplots(2, 1, figsize=(30, 15))
                        if fad_us_1_perjob_newuser.shape[0]>0:
                            fad_us_1_perjob_newuser[['PRECISION@5','RECALL@5','PRECISION@10','RECALL@10','PRECISION@20','RECALL@20']].plot.bar(ax=axes[0],title='Precision and recall for New Test Users - US Portal')
                            plot.legend(loc='upper right')
                        if fad_us_0_perjob_newuser.shape[0]>0:
                            fad_us_0_perjob_newuser[['PRECISION@5','RECALL@5','PRECISION@10','RECALL@10','PRECISION@20','RECALL@20']].plot.bar(ax=axes[1],title='Precision and recall for New non-test Users - US Portal')
                            plot.legend(loc='upper right')
                        plot.text(0,2.30,"Stats for US portal between test and non-test new users only", fontsize=20, color='red')
                        pdf.savefig()
                        plot.close()
                        ##############################################################
                        # Please note the range of the two plots could be different
                        # Comparision of total activities for each job for new users
                        figure, axes = plot.subplots(2, 1, figsize=(30, 15))
                        if fad_us_1_perjob_newuser.shape[0]>0:
                            fad_us_1_perjob_newuser[uniqueactivities].plot.bar(ax=axes[0],title='Overall activities for New Test Users - US Portal')
                            plot.legend(loc='upper right')
                        if fad_us_0_perjob_newuser.shape[0]>0:
                            fad_us_0_perjob_newuser[uniqueactivities].plot.bar(ax=axes[1],title='Overall activities for New non-test Users - US Portal')
                            plot.legend(loc='upper right')
                        plot.text(0,2.30,"Stats for US portal between test and non-test new users only", fontsize=20, color='red')
                        pdf.savefig()
                        plot.close()
                        ##############################################################
                        # Comparision of precision and recall and all activities overall for new users
                        fad_us_1_newuser_series = fad_us_1_perjob_newuser.agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_us_0_newuser_series = fad_us_0_perjob_newuser.agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_us_1_newuser = pd.DataFrame(fad_us_1_newuser_series)
                        fad_us_0_newuser = pd.DataFrame(fad_us_0_newuser_series)

                        fad_us_merged_newuser = pd.merge(fad_us_1_newuser, fad_us_0_newuser, left_index=True, right_index=True)
                        fad_us_merged_newuser.rename(columns = {'0_x':'TestUser', '0_y':'Non-testUser'}, inplace = True)
                        
                        figure, axes = plot.subplots(2, 1, figsize=(30, 15))
                        xx = fad_us_merged_newuser[:6].plot.bar(ax=axes[0],title='Overall precision and recall for new users - US Portal')
                        xx.set_xticklabels( fad_us_merged_newuser[:6].index.tolist(),rotation=0)    
                        yy = fad_us_merged_newuser[6:].plot.bar(ax=axes[1],title='Overall count of activities for new users - US Portal')
                        yy.set_xticklabels( fad_us_merged_newuser[6:].index.tolist(),rotation=0)
                        plot.text(-0.50,0.2,"Stats for US portal between test and non-test new users only", fontsize=20, color='red')
                        pdf.savefig()
                        plot.close()
                        ##############################################################
                        fad_all_1_perportal = final_analysis_df[(final_analysis_df['IS_TEST_USER']==1) & (final_analysis_df['AIML_JOB_RUN_ID']!=0)].groupby(['PORTAL']).agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_all_0_perportal = final_analysis_df[(final_analysis_df['IS_TEST_USER']==0) & (final_analysis_df['AIML_JOB_RUN_ID']!=0)].groupby(['PORTAL']).agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_all_1_perportal_past = final_analysis_df[(final_analysis_df['IS_TEST_USER']==1) & (final_analysis_df['AIML_JOB_RUN_ID']==0)].groupby(['PORTAL']).agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        ##############################################################
                        # Precision and recall for all Test user, here are not comparing it with non-test users as item for them appears randombly
                        fad_all_1_perportal[['PRECISION@5','RECALL@5','PRECISION@10','RECALL@10','PRECISION@20','RECALL@20']].plot.bar(figsize=(30,17),title='Portal wise precision and recall for Test users')
                        plot.legend(loc='upper right')
                        plot.text(-0.50,1.1,"Stats for all portals between test and non-test users", fontsize=20, color='red')
                        pdf.savefig()
                        plot.close()
                        ##############################################################
                        #Portal wise comparision between test and non-test users for all activites
                        for index, row in fad_all_1_perportal.iterrows():
                            figure, axes = plot.subplots(1, 3, figsize=(35, 17))
                            if fad_all_1_perportal[uniqueactivities].filter(like = index, axis=0).shape[0] > 0:
                                fad_all_1_perportal[uniqueactivities].filter(like = index, axis=0).plot.bar(ax=axes[0],title='Count of activities by All Test users - All Portal')        
                            if fad_all_0_perportal[uniqueactivities].filter(like = index, axis=0).shape[0] > 0:
                                fad_all_0_perportal[uniqueactivities].filter(like = index, axis=0).plot.bar(ax=axes[1],title='Count of activities by All Non-test users - All Portal')
                            if fad_all_1_perportal_past[uniqueactivities].filter(like = index, axis=0).shape[0] > 0:
                                fad_all_1_perportal_past[uniqueactivities].filter(like = index, axis=0).plot.bar(ax=axes[2],title='Count of past activities by All Test users - All Portal')        
                            #plot.text(-2.9,510,"Stats for all portals between test and non-test users", fontsize=20, color='red')
                            pdf.savefig()
                            plot.close()
                        ##############################################################
                        # Comparision of precision and recall and all activities overall
                        fad_all_1_series = fad_all_1_perportal.agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_all_0_series = fad_all_0_perportal.agg({'PRECISION@5':'mean', 'RECALL@5':'mean','PRECISION@10':'mean', 'RECALL@10':'mean', 'PRECISION@20':'mean', 'RECALL@20':'mean', 'ENTERCAMPAIGNOVERVIEW':'sum' , 'PULL':'sum', 'OPENASSETPREVIEW':'sum', 'ACTIVATECAMPAIGN':'sum', 'CAMPAIGNSTATUS':'sum', 'SETUPASSETS':'sum'})
                        fad_all_1 = pd.DataFrame(fad_all_1_series)
                        fad_all_0 = pd.DataFrame(fad_all_0_series)

                        fad_all_merged = pd.merge(fad_all_1, fad_all_0, left_index=True, right_index=True)
                        fad_all_merged.rename(columns = {'0_x':'TestUser', '0_y':'Non-testUser'}, inplace = True)
                        
                        figure, axes = plot.subplots(2, 1, figsize=(30, 15))
                        xx = fad_all_merged[:6].plot.bar(ax=axes[0],title='Overall precision and recall comparision - All Portal')
                        xx.set_xticklabels( fad_all_merged[:6].index.tolist(),rotation=0)
                        yy = fad_all_merged[6:].plot.bar(ax=axes[1],title='Overall count of activities comparision - All Portal')
                        yy.set_xticklabels( fad_all_merged[6:].index.tolist(),rotation=0)
                        plot.text(-0.50,1550,"Stats for all portals between test and non-test users", fontsize=20, color='red')
                        pdf.savefig()
                        plot.close()
                        ##############################################################
                        df_session_analysis = latestactivitydf[['USER_ID','LIBTRACKINGSESSIONID','ITEM_ID','IS_AIML_TEST_USER']].groupby(['USER_ID','LIBTRACKINGSESSIONID','IS_AIML_TEST_USER'], as_index=False).count()
                        df_session_analysis = df_session_analysis[['USER_ID','LIBTRACKINGSESSIONID','IS_AIML_TEST_USER']].groupby(['USER_ID','IS_AIML_TEST_USER'],as_index=False).count()
                        ##############################################################
                        #returning users
                        ret_user_plot = df_session_analysis[df_session_analysis['LIBTRACKINGSESSIONID']>1]['IS_AIML_TEST_USER'].value_counts().plot.bar(figsize=(10,10),title='Count of Returning users - All Portal')
                        ret_user_plot.set_xticklabels( ('Test users', 'Non-test users'),rotation=0)
                        pdf.savefig()
                        plot.close()
                        ##############################################################
                    ##########################################################################################
                except Exception as error:
                    print("Exception occurred while generating reports!")
                    raise
            except Exception as error: 
                raise
    except Exception as error:
        #print("Exception occurred: ", error)
        logger.error("Exception occurred", exc_info=True)
        raise
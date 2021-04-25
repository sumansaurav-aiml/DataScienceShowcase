# import the module
from sqlalchemy import create_engine
import ImportActivityDataForIngram
import ImportPortalItemMappingForIngram
import ImportItemBagOfWordsForIngram
import ExportTopHybridRecForPortalsUsers
import ExportTopOwnPastRatedProdForPortalsUsers
import ExportTopColFilterRecProdForPortalsUsers
import ExportTopTrendingProdForPortals
import ExportTopProdContBased
import RecommenderSystemForIngramHybridModel
import ExportAnalysisData
import os
#import logging
#logging.basicConfig(level=print, format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
try:
    print("Reading Creds from Envi!")
    dp = os.environ.get('APP_DBPROVIDER')
    un = os.environ.get('APP_USER')
    pw = os.environ.get('APP_PASSWORD')
    cs = os.environ.get('APP_CONNECTIONSTRING')
    
    conn_string = dp+'://'+un+':'+pw+'@'+cs
    
    # create sqlalchemy engine
    jobname ='RecommendationForIngram'
    status = 'Running'
    engine = create_engine(conn_string)
    with engine.connect() as conn:    
        print("You're connected!")
        print("Inserting Job run entry in database and getting jobid!")
        try:
            tran = conn.begin()
            data = conn.execute("""
                    INSERT INTO AIML_JOB_RUN_STATUS (AIML_JOB_RUN_ID,AIML_JOB_ID,MODEL_VERSION,STATUS) 
                    VALUES (SEQ_AIML_JOB_RUN_ID.nextval, (select AIML_JOB_ID from AIML_JOBS where AIML_JOB_NAME='"""+jobname+"""'),(select MODEL_VERSION from AIML_JOBS where AIML_JOB_NAME='"""+jobname+"""'),'"""+status+"""')
                    """)
            data = conn.execute("""SELECT MAX(AIML_JOB_RUN_ID) AS AIML_JOB_RUN_ID FROM AIML_JOB_RUN_STATUS
                    WHERE AIML_JOB_ID = (select AIML_JOB_ID from AIML_JOBS where AIML_JOB_NAME='"""+jobname+"""')
                    """)        
            tran.commit()
            jobid = data.fetchone()[0]
            print("Job Started Successfully. JobId: {}".format(jobid))
        except Exception as error:
            print("Error occurred, rolling back!")
            tran.rollback()
            raise
        finally:
            print("Closing Connection!")
            conn.close() 
    try:
        print("Creating Folder and SubFolder to save the Files for JobId: {}".format(jobid))
        parent_dir = "./data/jobid"+str(jobid)
        pred_directory = "predictionfiles"
        path = os.path.join(parent_dir, pred_directory) 
        os.makedirs(path, exist_ok = True)

        train_dir = "trainingfiles" 
        path = os.path.join(parent_dir, train_dir) 
        os.makedirs(path, exist_ok = True)
        print("Folders created successfully")
    except OSError as error:
        print("Exception occurred while creating folder JobId: {}".format(jobid))
        raise    
    try:
        
        print("Started import for ImportActivityDataForIngram")
        ImportActivityDataForIngram.ImportActivityDataForIngram(str(jobid),conn_string)
        print("Finished import for ImportActivityDataForIngram")
        """
        print("Started import for ImportPortalItemMappingForIngram")
        ImportPortalItemMappingForIngram.ImportPortalItemMappingForIngram(str(jobid),conn_string)
        print("Finished import for ImportPortalItemMappingForIngram")
        """
        print("Started import for ImportItemBagOfWordsForIngram")
        ImportItemBagOfWordsForIngram.ImportItemBagOfWordsForIngram(str(jobid),conn_string)
        print("Finished import for ImportItemBagOfWordsForIngram")
        
        print("Started RecommenderSystemForIngramHybridModel")
        RecommenderSystemForIngramHybridModel.RecommenderSystemForIngramHybridModel(str(jobid))
        print("Finished RecommenderSystemForIngramHybridModel")
        
        print("Started export for ExportTopHybridRecForPortalsUsers")
        ExportTopHybridRecForPortalsUsers.ExportTopHybridRecForPortalsUsers(str(jobid),conn_string)
        print("Finished import for ExportTopHybridRecForPortalsUsers")

        print("Started export for ExportTopOwnPastRatedProdForPortalsUsers")
        ExportTopOwnPastRatedProdForPortalsUsers.ExportTopOwnPastRatedProdForPortalsUsers(str(jobid),conn_string)
        print("Finished import for ExportTopOwnPastRatedProdForPortalsUsers")

        print("Started export for ExportTopColFilterRecProdForPortalsUsers")
        ExportTopColFilterRecProdForPortalsUsers.ExportTopColFilterRecProdForPortalsUsers(str(jobid),conn_string)
        print("Finished import for ExportTopColFilterRecProdForPortalsUsers")

        print("Started export for ExportTopTrendingProdForPortals")
        ExportTopTrendingProdForPortals.ExportTopTrendingProdForPortals(str(jobid),conn_string)
        print("Finished import for ExportTopTrendingProdForPortals")
        
        print("Started export for ExportTopProdContBased")
        ExportTopProdContBased.ExportTopProdContBased(str(jobid),conn_string)
        print("Finished import for ExportTopProdContBased")

    except Exception as error:
        print("Exception occurred in Import Export Model run process: ", error)
        status = 'Failed'
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            try:
                tran = conn.begin()
                data = conn.execute("""
                        UPDATE AIML_JOB_RUN_STATUS 
                        SET FINISH_DATE_TIME = SYSDATE,
                        STATUS='"""+status+"""',
                        STATUS_MESSAGE='"""+str(error).replace("'", "''")+"""'
                        WHERE AIML_JOB_RUN_ID="""+str(jobid)+"""
                        """)           
                tran.commit()
            except Exception as error:
                print("Error occurred, rolling back!")
                tran.rollback()
                raise
            finally:
                print("Closing Connection!")
                conn.close()
                raise
    else:
        status = 'Completed'
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            try:
                tran = conn.begin()
                data = conn.execute("""
                        UPDATE AIML_JOB_RUN_STATUS 
                        SET FINISH_DATE_TIME = SYSDATE,
                        STATUS='"""+status+"""'
                        WHERE AIML_JOB_RUN_ID="""+str(jobid)+"""
                        """)           
                tran.commit()
            except Exception as error:
                print("Error occurred, rolling back!")
                tran.rollback()
                raise
            finally:
                print("Closing Connection!")
                conn.close()
            try:
                print("Started export for ExportAnalysisData")
                ExportAnalysisData.ExportAnalysisData(str(jobid),conn_string)
                print("Finished import for ExportAnalysisData")
            except Exception as error:
                print("Exception while running ExportAnalysisData: ", error)
                raise
except Exception as error:    
    print("Exception occurred: ", error)
    raise
else:
    #print("Data imported successfully!!!")
    print("Job {} finished successfully!!!".format(jobid))
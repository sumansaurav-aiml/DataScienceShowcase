# import the module
from sqlalchemy import create_engine
import sqlalchemy as sa
import pandas as pd
import logging

def ExportTopHybridRecForPortalsUsers(jobid,conn_string):
    logging.basicConfig(level=logging.INFO, filename='./data/jobid'+str(jobid)+'/JobId'+jobid+'.log', filemode='a', format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger("ExportTopHybridRecForPortalsUsers")

    try:
        df = pd.read_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOP_HYBRID_REC_FOR_PORTALS_USERS.CSV')
        if df.shape[0] > 0:    
            #modify the column as per table
            df["AIML_JOB_RUN_ID"] = jobid 
            df["AIML_JOB_RUN_ID"] = pd.to_numeric(df["AIML_JOB_RUN_ID"])    
            df["USER_ID"] = pd.to_numeric(df["USER_ID"])    
            df["SORT_ORDER"] = pd.to_numeric(df["SORT_ORDER"])
            object_columns = [c for c in df.columns[df.dtypes == 'object'].tolist()]
            dtyp = {c:sa.types.VARCHAR(df[c].str.len().max()) for c in object_columns}
            try:
                # create sqlalchemy engine
                engine = create_engine(conn_string)
                with engine.connect() as conn:
                    logger.info("You're connected!")
                    try:
                        #print("Transaction begins!")
                        logger.info("Transaction begins!")
                        tran = conn.begin()
                        logger.info("Truncating table HYBRID_RECFOR_PORTAL_USER!")
                        data = conn.execute("""TRUNCATE TABLE HYBRID_RECFOR_PORTAL_USER""")                        
                        logger.info("Inserting data into HYBRID_RECFOR_PORTAL_USER!")
                        df.to_sql('HYBRID_RECFOR_PORTAL_USER', con = engine, if_exists = 'append', chunksize = 1000,index=False, dtype=dtyp) # takes 45 sec for 100000 records
                        logger.info("Inserting data into HYBRID_RECFOR_PORTAL_USER_HIST!")
                        data = conn.execute("""INSERT INTO HYBRID_RECFOR_PORTAL_USER_HIST (USER_ID,ITEM_ID,SORT_ORDER,PORTAL,MODEL_TYPE,AIML_JOB_RUN_ID)
                        SELECT USER_ID,ITEM_ID,SORT_ORDER,PORTAL,MODEL_TYPE,AIML_JOB_RUN_ID FROM HYBRID_RECFOR_PORTAL_USER""")
                        #print("Rows inserted successfully: ", df.shape[0])
                        logger.info("Rows inserted successfully: {}".format(df.shape[0]))                        
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
            raise Exception("Data not found in the csv file: TOP_HYBRID_REC_FOR_PORTALS_USERS")
    except Exception as error:
        #print("Exception occurred: ", error)
        logger.error("Exception occurred", exc_info=True)
        raise
    else:
        #print("Data exported successfully!!!")
        logger.info("Data exported successfully!!!")


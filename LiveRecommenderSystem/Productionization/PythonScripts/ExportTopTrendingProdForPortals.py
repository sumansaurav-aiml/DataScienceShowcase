# import the module
from sqlalchemy import create_engine
import sqlalchemy as sa
import pandas as pd
import logging

def ExportTopTrendingProdForPortals(jobid,conn_string):
    logging.basicConfig(level=logging.INFO, filename='./data/jobid'+str(jobid)+'/JobId'+jobid+'.log', filemode='a', format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger("ExportTopTrendingProdForPortals")

    try:
        df = pd.read_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOP_TRENDING_PROD_FOR_PORTALS.CSV')
        if df.shape[0] > 0:    
            #modify the column as per table
            df["AIML_JOB_RUN_ID"] = jobid 
            df["AIML_JOB_RUN_ID"] = pd.to_numeric(df["AIML_JOB_RUN_ID"])   
            df["RATING"] = pd.to_numeric(df["RATING"])
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
                        logger.info("Truncating table TRENDING_PROD_FOR_PORTALS!")
                        data = conn.execute("""TRUNCATE TABLE TRENDING_PROD_FOR_PORTALS""")                        
                        logger.info("Inserting data into TRENDING_PROD_FOR_PORTALS!")
                        df.to_sql('TRENDING_PROD_FOR_PORTALS', con = engine, if_exists = 'append', chunksize = 1000,index=False, dtype=dtyp) # takes 45 sec for 100000 records
                        logger.info("Inserting data into TRENDING_PROD_FOR_PORTALS_HIST!")
                        data = conn.execute("""INSERT INTO TRENDING_PROD_FOR_PORTALS_HIST (ITEM_ID,RATING,PORTAL,AIML_JOB_RUN_ID)
                        SELECT ITEM_ID,RATING,PORTAL,AIML_JOB_RUN_ID FROM TRENDING_PROD_FOR_PORTALS""")
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
            raise Exception("Data not found in the csv file: TOP_TRENDING_PROD_FOR_PORTALS.csv")
    except Exception as error:
        #print("Exception occurred: ", error)
        logger.error("Exception occurred", exc_info=True)
        raise
    else:
        #print("Data exported successfully!!!")
        logger.info("Data exported successfully!!!")


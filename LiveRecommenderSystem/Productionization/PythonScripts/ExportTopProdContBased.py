# import the module
from sqlalchemy import create_engine
import sqlalchemy as sa
import pandas as pd
import logging

def ExportTopProdContBased(jobid,conn_string):
    logging.basicConfig(level=logging.INFO, filename='./data/jobid'+str(jobid)+'/JobId'+jobid+'.log', filemode='a', format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger("ExportTopProdContBased")

    try:
        df = pd.read_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOP_PROD_CONT_BASED.CSV')
        if df.shape[0] > 0:    
            #modify the column as per table
            df["AIML_JOB_RUN_ID"] = jobid 
            df["AIML_JOB_RUN_ID"] = pd.to_numeric(df["AIML_JOB_RUN_ID"])   
            
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
                        logger.info("Truncating table PROD_CONT_BASED_REC!")
                        data = conn.execute("""TRUNCATE TABLE PROD_CONT_BASED_REC""")                        
                        logger.info("Inserting data into PROD_CONT_BASED_REC!")
                        df.to_sql('PROD_CONT_BASED_REC', con = engine, if_exists = 'append', chunksize = 1000,index=False, dtype=dtyp) # takes 45 sec for 100000 records
                        logger.info("Inserting data into PROD_CONT_BASED_REC_HIST!")
                        data = conn.execute("""INSERT INTO PROD_CONT_BASED_REC_HIST (ITEM_ID,RECOMMENDED_ITEM_ID,AIML_JOB_RUN_ID)
                        SELECT ITEM_ID,RECOMMENDED_ITEM_ID,AIML_JOB_RUN_ID FROM PROD_CONT_BASED_REC""")
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
            raise Exception("Data not found in the csv file: TOP_PROD_CONT_BASED.csv")
    except Exception as error:
        #print("Exception occurred: ", error)
        logger.error("Exception occurred", exc_info=True)
        raise
    else:
        #print("Data exported successfully!!!")
        logger.info("Data exported successfully!!!")


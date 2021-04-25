# import the module
from sqlalchemy import create_engine
import pandas as pd
import logging

def ImportPortalItemMappingForIngram(jobid,conn_string):
    logging.basicConfig(level=logging.INFO, filename='./data/jobid'+str(jobid)+'/JobId'+jobid+'.log', filemode='a', format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger("ImportPortalItemMappingForIngram")

    try:
        # create sqlalchemy engine
        engine = create_engine(conn_string)
        with engine.connect() as conn:    
            logger.info("You're connected!")
            logger.info("Fetching User Item Portal data!")
            data = conn.execute("""
                    SELECT
                    upper(p.name) AS NAME,
                    pm.PRODUCTMASTERID AS ITEM_ID
                FROM
                    customer.portal_productmaster   ppm
                    JOIN customer.productmaster          pm ON ppm.productmasterid = pm.productmasterid
                    JOIN customer.portal                 p ON p.portalid = ppm.portalid
                WHERE
                    p.managingcompanysiteid = 65774""")

            all_rows=data.fetchall()
            try:
                df=pd.DataFrame(list(all_rows),columns=["NAME","ITEM_ID"])
                logger.info("Saving data to csv:{}".format('PORTAL_ITEM_MAPPING.CSV'))
                df.to_csv("./data/jobid"+str(jobid)+"/trainingfiles/PORTAL_ITEM_MAPPING.CSV",index=False)
            except Exception as error: 
                raise
    except Exception as error:
        #print("Exception occurred: ", error)
        logger.error("Exception occurred", exc_info=True)
        raise
    else:
        #print("Data imported successfully!!!")
        logger.info("Data imported successfully!!!")
    finally:
        logger.info("Closing Connection!")
        conn.close()
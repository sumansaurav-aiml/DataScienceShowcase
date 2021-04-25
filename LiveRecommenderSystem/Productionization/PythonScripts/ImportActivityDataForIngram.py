# import the module
from sqlalchemy import create_engine
import pandas as pd
import logging

def ImportActivityDataForIngram(jobid,conn_string):
    logging.basicConfig(level=logging.INFO, filename='./data/jobid'+str(jobid)+'/JobId'+jobid+'.log', filemode='a', format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger("ImportActivityDataForIngram")

    try:
        # create sqlalchemy engine
        engine = create_engine(conn_string)
        with engine.connect() as conn:    
            logger.info("You're connected!")
            logger.info("Fetching User Activity data!")
            data = conn.execute("""
            SELECT DISTINCT
                lts.userid     AS "USER_ID",
                pm.PRODUCTMASTERID AS "ITEM_ID",
                TRIM(UPPER(lta.type))       AS "ACTIVITY_TYPE",
                lta.datetime   AS "TIMESTAMP",
                (
                    SELECT
                        UPPER(p.name) AS NAME
                    FROM
                        customer.managedbundlecompanysite   mbc
                        JOIN customer.portal_program             pp ON ( mbc.managedbundleid = pp.programid
                                                            AND pp.program_type_id = 2 )
                        JOIN customer.portal                     p ON p.portalid = pp.portalid
                    WHERE
                        mbc.companysiteid = lts.companysiteid
                        AND p.name IS NOT NULL
                ) AS portalname,
                lts.LIBTRACKINGSESSIONID,
                (select IS_AIML_TEST_USER from framewrk.companysiteuser csu where csu.userid = lts.userid and csu.companysiteid=lts.companysiteid) IS_AIML_TEST_USER,
                TRIM(UPPER(nvl(pm.displayname, pm.name))) as "ITEM_NAME"
            FROM
                customer.libtrackingactivity         lta
                JOIN customer.libtrackingsession          lts ON lta.libtrackingsessionid = lts.libtrackingsessionid
                                                        AND nvl(lts.internaluser, 0) = 0
                                                        AND lts.libcompanyid = 65774
                                                        AND lta.campaignid != - 1
                                                        AND lta.campaignid IS NOT NULL
                JOIN framewrk.users                       u ON u.usersid = lts.userid
                                        AND u.creatorcompanysiteid = lts.companysiteid
                        ----------------------------------
                JOIN framewrk.companysiteregistration     csr ON lts.companysiteid = csr.companysiteid
                                                            AND nvl(csr.testsite, 0) = 0
                JOIN customer.campaign_plans              cp ON cp.campaign_plan_id = lta.campaignid
                JOIN customer.product_instance_campaign   pic ON pic.campaign_plan_id = decode(cp.managed_campaign_plan_id, NULL, cp.campaign_plan_id
                , 0, cp.campaign_plan_id,
                                                                                            cp.managed_campaign_plan_id)
                JOIN customer.product_instance            pi ON pi.product_instance_id = pic.product_instance_id
                JOIN customer.productmaster               pm ON pi.product_master_id = pm.productmasterid
                LEFT JOIN framewrk.usercontactemail            uce ON uce.userid = u.usersid
                LEFT JOIN framewrk.email                       e ON e.emailid = uce.emailid
            WHERE
                lts.libcompanyid = 65774
                AND lts.companysiteid != lts.libcompanyid
                AND lower(emailaddress) NOT LIKE '%@ingrammicro.com%'
            UNION
            SELECT DISTINCT
                lts.userid     AS "USER_ID",
                pm.PRODUCTMASTERID AS "ITEM_ID",
                TRIM(UPPER(lta.type))       AS "ACTIVITY_TYPE",
                lta.datetime   AS "TIMESTAMP",
                (
                    SELECT
                        UPPER(p.name) AS NAME
                    FROM
                        customer.managedbundlecompanysite   mbc
                        JOIN customer.portal_program             pp ON ( mbc.managedbundleid = pp.programid
                                                            AND pp.program_type_id = 2 )
                        JOIN customer.portal                     p ON p.portalid = pp.portalid
                    WHERE
                        mbc.companysiteid = lts.companysiteid
                        AND p.name IS NOT NULL
                ) AS portalname,
                lts.LIBTRACKINGSESSIONID,
                (select IS_AIML_TEST_USER from framewrk.companysiteuser csu where csu.userid = lts.userid and csu.companysiteid=lts.companysiteid) IS_AIML_TEST_USER,
                TRIM(UPPER(nvl(pm.displayname, pm.name))) as "ITEM_NAME"
            FROM
                customer.libtrackingactivity       lta
                JOIN customer.libtrackingsession        lts ON lta.libtrackingsessionid = lts.libtrackingsessionid
                                                        AND nvl(lts.internaluser, 0) = 0
                                                        AND lts.libcompanyid = 65774
                                                        AND lta.campaignid = - 1
                                                        AND lta.type = 'EnterProduct'
                JOIN framewrk.users                     u ON u.usersid = lts.userid
                                        AND u.creatorcompanysiteid = lts.companysiteid
                        ----------------------------------
                JOIN framewrk.companysiteregistration   csr ON lts.companysiteid = csr.companysiteid
                                                            AND nvl(csr.testsite, 0) = 0
                JOIN customer.product_instance          pi ON pi.product_instance_id = lta.value
                JOIN customer.productmaster             pm ON pi.product_master_id = pm.productmasterid
                LEFT JOIN framewrk.usercontactemail          uce ON uce.userid = u.usersid
                LEFT JOIN framewrk.email                     e ON e.emailid = uce.emailid
            WHERE
                lts.libcompanyid = 65774
                AND lts.companysiteid != lts.libcompanyid
                AND lower(emailaddress) NOT LIKE '%@ingrammicro.com%'
            UNION
            SELECT DISTINCT
                lts.userid     AS "USER_ID",
                pm.PRODUCTMASTERID AS "ITEM_ID",
                TRIM(UPPER(lta.type))       AS "ACTIVITY_TYPE",
                lta.datetime   AS "TIMESTAMP",
                (
                    SELECT
                        UPPER(p.name) AS NAME
                    FROM
                        customer.managedbundlecompanysite   mbc
                        JOIN customer.portal_program             pp ON ( mbc.managedbundleid = pp.programid
                                                            AND pp.program_type_id = 2 )
                        JOIN customer.portal                     p ON p.portalid = pp.portalid
                    WHERE
                        mbc.companysiteid = lts.companysiteid
                        AND p.name IS NOT NULL
                ) AS portalname,
                lts.LIBTRACKINGSESSIONID,
                (select IS_AIML_TEST_USER from framewrk.companysiteuser csu where csu.userid = lts.userid and csu.companysiteid=lts.companysiteid) IS_AIML_TEST_USER,
                TRIM(UPPER(nvl(pm.displayname, pm.name))) as "ITEM_NAME"
            FROM
                customer.libtrackingactivity         lta
                JOIN customer.libtrackingsession          lts ON lta.libtrackingsessionid = lts.libtrackingsessionid
                                                        AND nvl(lts.internaluser, 0) = 0
                                                        AND lts.libcompanyid = 65774
                                                        AND lta.campaignid = - 1
                                                        AND lta.type IN (
                    'ActivateCampaign',
                    'DownloadCampaign'
                )
                JOIN framewrk.users                       u ON u.usersid = lts.userid
                                        AND u.creatorcompanysiteid = lts.companysiteid
                        ----------------------------------
                JOIN framewrk.companysiteregistration     csr ON lts.companysiteid = csr.companysiteid
                                                            AND nvl(csr.testsite, 0) = 0
                JOIN customer.product_instance_campaign   pic ON pic.campaign_plan_id = lta.value
                JOIN customer.product_instance            pi ON pi.product_instance_id = pic.product_instance_id
                JOIN customer.productmaster               pm ON pi.product_master_id = pm.productmasterid
                LEFT JOIN framewrk.usercontactemail            uce ON uce.userid = u.usersid
                LEFT JOIN framewrk.email                       e ON e.emailid = uce.emailid
            WHERE
                lts.libcompanyid = 65774
                AND lts.companysiteid != lts.libcompanyid
                AND lower(emailaddress) NOT LIKE '%@ingrammicro.com%'""")

            all_rows=data.fetchall()
            try:
                df=pd.DataFrame(list(all_rows),columns=["USER_ID","ITEM_ID","ACTIVITY_TYPE","TIMESTAMP","PORTALNAME","LIBTRACKINGSESSIONID","IS_AIML_TEST_USER","ITEM_NAME"])
                logger.info("Saving data to csv:{}".format('USER_ACTIVITY_WITH_TIME_PORTAL.CSV'))
                df.to_csv("./data/jobid"+str(jobid)+"/trainingfiles/USER_ACTIVITY_WITH_TIME_PORTAL.CSV",index=False)
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


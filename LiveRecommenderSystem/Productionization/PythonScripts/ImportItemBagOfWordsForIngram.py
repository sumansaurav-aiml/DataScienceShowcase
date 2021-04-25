# import the module
from sqlalchemy import create_engine
import pandas as pd
import logging

def ImportItemBagOfWordsForIngram(jobid,conn_string):
    logging.basicConfig(level=logging.INFO, filename='./data/jobid'+str(jobid)+'/JobId'+jobid+'.log', filemode='a', format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger("ImportItemBagOfWordsForIngram")

    try:
        # create sqlalchemy engine
        engine = create_engine(conn_string)
        with engine.connect() as conn:    
            logger.info("You're connected!")
            logger.info("Fetching User Item Portal data!")
            data = conn.execute("""
                    SELECT PRODUCTMASTERID AS ITEM_ID,UPPER(ITEM_ID ||' '||TARGET_BUSINESS_SIZE||' '||TARGET_INDUSTRY||' '||CATEGORY||' '||VENDOR||' '||REPLACE(REPLACE(MARKETPLACES,' ',''),'|',' ')||' '||SOLUTION_GROUP) AS BAG_OF_WORDS
            FROM (
            SELECT DISTINCT
                pm.PRODUCTMASTERID,
                TRIM(nvl(
                    pm.displayname,
                    pm.name
                )) AS "ITEM_ID",
                nvl(
                    camp_attr4."TARGET_BUSINESS_SIZE",
                    'All'
                ) AS "TARGET_BUSINESS_SIZE",
                nvl(
                    camp_attr3."TARGET_INDUSTRY",
                    'All'
                ) AS "TARGET_INDUSTRY",
                nvl(
                    camp_attr2."CATEGORY",
                    'All'
                ) AS "CATEGORY",
                nvl(
                    camp_attr."VENDOR",
                    'NA'
                ) AS "VENDOR",
                nvl(
                    m.marketplaces,
                    'All'
                ) AS marketplaces,
                nvl(
                    "SOLUTION_GROUP",
                    'All'
                ) AS solution_group
            FROM
                customer.productmaster pm
                JOIN customer.product_instance pi ON
                    pi.product_master_id = pm.productmasterid
                AND
                    pi.is_active_version = 1
                AND
                    pm.inactive = 0
                JOIN customer.product_instance_campaign pic ON pi.product_instance_id = pic.product_instance_id
                JOIN customer.campaign_plans cp ON
                    pic.campaign_plan_id = cp.campaign_plan_id
                AND
                    cp.companysiteid = 65774
            --=====================================
            -->>bring Target Business Size for ProductMasterID
                LEFT JOIN (
                    SELECT DISTINCT
                        productmasterid,
                        LISTAGG(
                            "'Target Business Size'",
                            '|'
                        ) WITHIN GROUP(ORDER BY productmasterid) AS "TARGET_BUSINESS_SIZE"
                    FROM
                        (
                            SELECT DISTINCT
                                pm.productmasterid,
                                catt.attname,
                                cas.subname
                            FROM
                                customer.campaign_plans cp
                                JOIN customer.product_instance_campaign pic ON cp.campaign_plan_id = pic.campaign_plan_id
                                JOIN customer.product_instance pi ON pi.product_instance_id = pic.product_instance_id
                                JOIN customer.productmaster pm ON
                                    pi.product_master_id = pm.productmasterid
                                AND
                                    pi.is_active_version = 1
                                AND
                                    pm.inactive = 0
                                LEFT JOIN customer.campaignattributesubselect cass ON cass.campaign_plan_id = cp.campaign_plan_id
                                LEFT JOIN customer.campaignattributesub cas ON cas.campaignattributesubid = cass.campaignattributesubid
                                LEFT JOIN customer.campaignattribute catt ON
                                    catt.campaignattributeid = cas.campaignattributeid
                                AND
                                    catt.attname IS NOT NULL
                            WHERE
                                    cp.companysiteid = 65774
                                AND
                                    catt.attname IN (
                                        'Target Business Size'
                                    )
                        )
                            PIVOT ( MAX ( subname )
                                FOR attname
                                IN ( 'Target Business Size' )
                            )
                    GROUP BY
                        productmasterid
                ) camp_attr4 ON camp_attr4.productmasterid = pm.productmasterid
            --=====================================
            -->>bring Target Industry for ProductMasterID
                LEFT JOIN (
                    SELECT DISTINCT
                        productmasterid,
                        LISTAGG(
                            "'Target Industry'",
                            '|'
                        ) WITHIN GROUP(ORDER BY productmasterid) AS "TARGET_INDUSTRY"
                    FROM
                        (
                            SELECT DISTINCT
                                pm.productmasterid,
                                catt.attname,
                                cas.subname
                            FROM
                                customer.campaign_plans cp
                                JOIN customer.product_instance_campaign pic ON cp.campaign_plan_id = pic.campaign_plan_id
                                JOIN customer.product_instance pi ON pi.product_instance_id = pic.product_instance_id
                                JOIN customer.productmaster pm ON
                                    pi.product_master_id = pm.productmasterid
                                AND
                                    pi.is_active_version = 1
                                AND
                                    pm.inactive = 0
                                LEFT JOIN customer.campaignattributesubselect cass ON cass.campaign_plan_id = cp.campaign_plan_id
                                LEFT JOIN customer.campaignattributesub cas ON cas.campaignattributesubid = cass.campaignattributesubid
                                LEFT JOIN customer.campaignattribute catt ON
                                    catt.campaignattributeid = cas.campaignattributeid
                                AND
                                    catt.attname IS NOT NULL
                            WHERE
                                    cp.companysiteid = 65774
                                AND
                                    catt.attname IN (
                                        'Target Industry'
                                    )
                        )
                            PIVOT ( MAX ( subname )
                                FOR attname
                                IN ( 'Target Industry' )
                            )
                    GROUP BY
                        productmasterid
                ) camp_attr3 ON camp_attr3.productmasterid = pm.productmasterid
            --=====================================
            -->>bring Category for ProductMasterID
                LEFT JOIN (
                    SELECT DISTINCT
                        productmasterid,
                        LISTAGG(
                            "'Category'",
                            '|'
                        ) WITHIN GROUP(ORDER BY productmasterid) AS "CATEGORY"
                    FROM
                        (
                            SELECT DISTINCT
                                pm.productmasterid,
                                catt.attname,
                                cas.subname
                            FROM
                                customer.campaign_plans cp
                                JOIN customer.product_instance_campaign pic ON cp.campaign_plan_id = pic.campaign_plan_id
                                JOIN customer.product_instance pi ON pi.product_instance_id = pic.product_instance_id
                                JOIN customer.productmaster pm ON
                                    pi.product_master_id = pm.productmasterid
                                AND
                                    pi.is_active_version = 1
                                AND
                                    pm.inactive = 0
                                LEFT JOIN customer.campaignattributesubselect cass ON cass.campaign_plan_id = cp.campaign_plan_id
                                LEFT JOIN customer.campaignattributesub cas ON cas.campaignattributesubid = cass.campaignattributesubid
                                LEFT JOIN customer.campaignattribute catt ON
                                    catt.campaignattributeid = cas.campaignattributeid
                                AND
                                    catt.attname IS NOT NULL
                            WHERE
                                    cp.companysiteid = 65774
                                AND
                                    catt.attname IN (
                                        'Category'
                                    )
                        )
                            PIVOT ( MAX ( subname )
                                FOR attname
                                IN ( 'Category' )
                            )
                    GROUP BY
                        productmasterid
                ) camp_attr2 ON camp_attr2.productmasterid = pm.productmasterid
            --=====================================
            -->>bring Vendors for ProductMasterID
                LEFT JOIN (
                    SELECT DISTINCT
                        productmasterid,
                        LISTAGG(
                            "'Vendor'",
                            '|'
                        ) WITHIN GROUP(ORDER BY productmasterid) AS "VENDOR"
                    FROM
                        (
                            SELECT DISTINCT
                                pm.productmasterid,
                                catt.attname,
                                cas.subname
                            FROM
                                customer.campaign_plans cp
                                JOIN customer.product_instance_campaign pic ON cp.campaign_plan_id = pic.campaign_plan_id
                                JOIN customer.product_instance pi ON pi.product_instance_id = pic.product_instance_id
                                JOIN customer.productmaster pm ON
                                    pi.product_master_id = pm.productmasterid
                                AND
                                    pi.is_active_version = 1
                                AND
                                    pm.inactive = 0
                                LEFT JOIN customer.campaignattributesubselect cass ON cass.campaign_plan_id = cp.campaign_plan_id
                                LEFT JOIN customer.campaignattributesub cas ON cas.campaignattributesubid = cass.campaignattributesubid
                                LEFT JOIN customer.campaignattribute catt ON
                                    catt.campaignattributeid = cas.campaignattributeid
                                AND
                                    catt.attname IS NOT NULL
                            WHERE
                                    cp.companysiteid = 65774
                                AND
                                    catt.attname IN (
                                        'Vendor'
                                    )
                        )
                            PIVOT ( MAX ( subname )
                                FOR attname
                                IN ( 'Vendor' )
                            )
                    GROUP BY
                        productmasterid
                ) camp_attr ON camp_attr.productmasterid = pm.productmasterid
            --========================================
            -->> brings the Marketplace for ProductMasterID
                JOIN (
                    SELECT
                        productmasterid -- nvl(pm.displayname,pm.name) as "ITEM_ID"
                        ,
                        LISTAGG(
                            bundlename,
                            '|'
                        ) WITHIN GROUP(ORDER BY productmasterid) AS marketplaces
                    FROM
                        (
                            SELECT DISTINCT
                                pm.productmasterid -- nvl(pm.displayname,pm.name) as "ITEM_ID"
                                ,
                                mb.bundlename
                            FROM
                                customer.campaign_plan_mb cpmb
                                JOIN customer.managedbundle mb ON
                                    cpmb.managedbundleid = mb.managedbundleid
                                AND
                                    mb.bundlecompanysiteid = 65774
                                AND
                                    mb.bundlename LIKE 'MP%'
                                JOIN customer.product_instance_campaign pic ON cpmb.campaign_plan_id = pic.campaign_plan_id
                                JOIN customer.product_instance pi ON pi.product_instance_id = pic.product_instance_id
                                JOIN customer.productmaster pm ON
                                    pi.product_master_id = pm.productmasterid
                                AND
                                    pi.is_active_version = 1
                                AND
                                    pm.inactive = 0
                        )
                    GROUP BY
                        productmasterid
                ) m ON m.productmasterid = pm.productmasterid
            -- m."ITEM_ID" = nvl(pm.displayname,pm.name)
            --=====================================
            -->>bring Solution Playbook Group for ProductMasterID
                LEFT JOIN (
                    SELECT DISTINCT
                        productmasterid,
                        LISTAGG(
                            "'Solution Group'",
                            '|'
                        ) WITHIN GROUP(ORDER BY productmasterid) AS "SOLUTION_GROUP"
                    FROM
                        (
                            SELECT DISTINCT
                                pm.productmasterid,
                                catt.attname,
                                cas.subname
                            FROM
                                customer.campaign_plans cp
                                JOIN customer.product_instance_campaign pic ON cp.campaign_plan_id = pic.campaign_plan_id
                                JOIN customer.product_instance pi ON pi.product_instance_id = pic.product_instance_id
                                JOIN customer.productmaster pm ON
                                    pi.product_master_id = pm.productmasterid
                                AND
                                    pi.is_active_version = 1
                                AND
                                    pm.inactive = 0
                                LEFT JOIN customer.campaignattributesubselect cass ON cass.campaign_plan_id = cp.campaign_plan_id
                                LEFT JOIN customer.campaignattributesub cas ON cas.campaignattributesubid = cass.campaignattributesubid
                                LEFT JOIN customer.campaignattribute catt ON
                                    catt.campaignattributeid = cas.campaignattributeid
                                AND
                                    catt.attname IS NOT NULL
                            WHERE
                                    cp.companysiteid = 65774
                                AND
                                    catt.attname IN (
                                        'Solution Group'
                                    )
                        )
                            PIVOT ( MAX ( subname )
                                FOR attname
                                IN ( 'Solution Group' )
                            )
                    GROUP BY
                        productmasterid
                ) camp_attr ON camp_attr.productmasterid = pm.productmasterid)""")

            all_rows=data.fetchall()
            try:
                df=pd.DataFrame(list(all_rows),columns=["ITEM_ID","BAG_OF_WORDS"])
                logger.info("Saving data to csv:{}".format('ITEM_BAG_OF_WORDS.CSV'))
                df.to_csv("./data/jobid"+str(jobid)+"/trainingfiles/ITEM_BAG_OF_WORDS.CSV",index=False)
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
#RecommenderSystemForIngramHybridModel file

## libraries
import numpy as np
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plot
import seaborn as sns
from collections import defaultdict
from surprise import Reader, Dataset, KNNWithMeans, accuracy
from surprise.model_selection import train_test_split
from sklearn import preprocessing
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from rake_nltk import Rake
import random
import logging

def RecommenderSystemForIngramHybridModel(jobid):
    logging.basicConfig(level=logging.INFO, filename='./data/jobid'+str(jobid)+'/JobId'+jobid+'.log', filemode='a', format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger("RecommenderSystemForIngramHybridModel")
    try:
        ########################ContentBased recommendation for each item######################
        logger.info("Starting Contentbased recommendation")
        logger.info("Reading data from csv file ITEM_BAG_OF_WORDS!")
        df_cont_based = pd.read_csv('./data/jobid'+str(jobid)+'/trainingfiles/ITEM_BAG_OF_WORDS.CSV')
        df_cont_based['ITEM_ID'] = df_cont_based['ITEM_ID'].apply(str)
        # initializing the new column
        df_cont_based['Key_words'] = ""

        for index, row in df_cont_based.iterrows():
            bow = row['BAG_OF_WORDS']

            # instantiating Rake, by default is uses english stopwords from NLTK
            # and discard all puntuation characters
            r = Rake()

            # extracting the words by passing the text
            r.extract_keywords_from_text(bow)

            # getting the dictionary whith key words and their scores
            key_words_dict_scores = r.get_word_degrees()

            # assigning the key words to the new column
            row['Key_words'] = list(key_words_dict_scores.keys())

        # dropping the Plot column
        df_cont_based.drop(columns = ['BAG_OF_WORDS'], inplace = True)
        df_cont_based.set_index('ITEM_ID', inplace = True)
        for index, row in df_cont_based.iterrows():
            row['Key_words'] = ' '.join(row['Key_words']).lower()
        
        # instantiating and generating the count matrix
        count = CountVectorizer()
        count_matrix = count.fit_transform(df_cont_based['Key_words'])

        # creating a Series for products so they are associated to an ordered numerical
        # list I will use later to match the indexes
        indices = pd.Series(df_cont_based.index)
        c=count_matrix.todense()
        # generating the cosine similarity matrix
        cosine_sim = cosine_similarity(count_matrix, count_matrix)
        # top 10 recommendation from Content based recommendation
        def recommendations(item, cosine_sim = cosine_sim):

            recommended_item = []

            idx = indices[indices == item].index[0]

            score_series = pd.Series(cosine_sim[idx]).sort_values(ascending = False)

            top_10_indexes = list(score_series.iloc[1:11].index)
            #print(top_10_indexes)

            for i in top_10_indexes:
                recommended_item.append(list(df_cont_based.index)[i])

            return recommended_item
        top_prod_cont_based = pd.DataFrame(columns=['ITEM_ID','RECOMMENDED_ITEM_ID'])
        for i in indices:
            cont_based_rec = recommendations(i)
            for j in cont_based_rec:
                new_row = {'ITEM_ID':i, 'RECOMMENDED_ITEM_ID': j}
                top_prod_cont_based = top_prod_cont_based.append(new_row,ignore_index=True)
        logger.info("Exporting recommendation to csv file TOP_PROD_CONT_BASED!")
        top_prod_cont_based.to_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOP_PROD_CONT_BASED.CSV',index=False)
        logger.info("Finished content based recommendation!")
        #######################Getting Top Rated products###########################
        #reading users activity data on items
        logger.info("Starting Top Trending recommendation")
        logger.info("Reading data from CSV file USER_ACTIVITY_WITH_TIME_PORTAL")
        df = pd.read_csv('./data/jobid'+str(jobid)+'/trainingfiles/USER_ACTIVITY_WITH_TIME_PORTAL.CSV')
        df.dropna(subset=['PORTALNAME'],inplace=True)
        #Since Download, Pull, Video have are essentially same action, hence combining them
        def combining_activation_download_actions(action):
            if action=='ACTIVATECAMPAIGN':
                return "ACTIVATECAMPAIGN"
            elif action == 'DOWNLOADCAMPAIGN':
                return "ACTIVATECAMPAIGN"        
            else:
                return action
        # Generalizing the activation_download_actions action.
        df['ACTIVITY_TYPE'] = df.apply(lambda x: combining_activation_download_actions(x['ACTIVITY_TYPE']),axis=1)
        #Since we have PDF downloads appended with tactics id, lets generalize them as "Download" action
        def generalizing_actions(action):
            if action.find("DOWNLOAD")>=0:
                return "DOWNLOAD"
            else:
                return action
        # Generalizing the pdf download action.
        df['ACTIVITY_TYPE'] = df.apply(lambda x: generalizing_actions(x['ACTIVITY_TYPE']),axis=1)
        #As we can see that "EnterCampaignStartmarketing" has been done only once, this seems to be a wrong data, lets remove it.
        df = df.loc[df['ACTIVITY_TYPE']!='ENTERCAMPAIGNSTARTMARKETING']
        #Also lets remove the "Close" actions 
        df = df.loc[df['ACTIVITY_TYPE']!='CLOSEASSETPREVIEW']
        df = df.loc[df['ACTIVITY_TYPE']!='CLOSESETUPASSETS']
        #Since Download, Pull, Video have are essentially same action, hence combining them
        def combining_pull_actions(action):
            if action=='VIDEO_GETDEFAULTEMBEDCODE':
                return "PULL"
            elif action == 'DOWNLOAD':
                return "PULL"        
            else:
                return action
        # Combining Download, Pull, Video.
        df['ACTIVITY_TYPE'] = df.apply(lambda x: combining_pull_actions(x['ACTIVITY_TYPE']),axis=1)
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
        df['ACTIVITY_TYPE'] = df.apply(lambda x: combining_enter_camp_actions(x['ACTIVITY_TYPE']),axis=1)
        # Count of different Actions overall
        df_action_by_item = df[['ACTIVITY_TYPE','ITEM_ID']].groupby(['ACTIVITY_TYPE'], as_index=False).count()
        df_action_by_item.rename(columns = {'ITEM_ID':'COUNT'}, inplace = True)
        #df_action_by_item
        # Decide on weight based on the number of actions dynamically.
        def assign_weight_to_actions(action, df_action_by_item):
            #hardcoding the number of actions    
            total_cnt_of_activity = df_action_by_item.COUNT.sum()    
            if action=='PULL':        
                return total_cnt_of_activity/df_action_by_item[df_action_by_item['ACTIVITY_TYPE']==action].iat[0,1]         
            elif action=='SETUPASSETS':        
                return total_cnt_of_activity/df_action_by_item[df_action_by_item['ACTIVITY_TYPE']==action].iat[0,1]
            elif action=='OPENASSETPREVIEW':
                return total_cnt_of_activity/df_action_by_item[df_action_by_item['ACTIVITY_TYPE']==action].iat[0,1]
            elif action=='ENTERCAMPAIGNOVERVIEW':
                return total_cnt_of_activity/df_action_by_item[df_action_by_item['ACTIVITY_TYPE']==action].iat[0,1]
            elif action=='CAMPAIGNSTATUS':
                return total_cnt_of_activity/df_action_by_item[df_action_by_item['ACTIVITY_TYPE']==action].iat[0,1]
            elif action=='ACTIVATECAMPAIGN':
                return total_cnt_of_activity/df_action_by_item[df_action_by_item['ACTIVITY_TYPE']==action].iat[0,1]
            else:
                return 0

        # this function takes acivity data frame as input and returns user-item score
        def return_user_item_rating(data):
            datawithwt = data.copy()
            # Count of different Actions overall
            df_action_by_item = datawithwt[['ACTIVITY_TYPE','ITEM_ID']].groupby(['ACTIVITY_TYPE'], as_index=False).count()
            df_action_by_item.rename(columns = {'ITEM_ID':'COUNT'}, inplace = True)
            #Applying the function to assign weights to each user action
            datawithwt['WEIGHT'] = datawithwt.apply(lambda x: assign_weight_to_actions(x['ACTIVITY_TYPE'],df_action_by_item),axis=1)
            #Getting total weight of each Item by each User and the number of actions
            datawithwt = datawithwt[['USER_ID','ITEM_ID','WEIGHT']].groupby(['USER_ID','ITEM_ID'], as_index=False)["WEIGHT"].agg({'count':"count", 'sum':sum}).reset_index()
            #Calculating the Weighted weight on each item by each user, to evaluate how many action a user took to produce this much weight on an item
            datawithwt['RATING'] = datawithwt[('sum')]/datawithwt[('count')]  
            
            #Lets normalize the data in the range of 1 to 5 using minmaxscaler
            mm_scaler = preprocessing.MinMaxScaler(feature_range=(1,5))
            #Scaling the Weights from 1 to 5 and storing it as Rating
            datawithwt['RATING'] = mm_scaler.fit_transform(datawithwt[['RATING']])
            datawithwt.drop(['count','sum','index'],axis=1,inplace=True)
            return datawithwt

        #getting user's item rating for each user per item per portal
        user_item_portal_rating = dict()
        portals = list(df['PORTALNAME'].unique())
        #portals.remove('INGRAM CLOUDBLUE DEMO HUB')
        for portal in portals:
            #print('\nportal name - ',portal)
            portal_data = df[df['PORTALNAME'] == portal] # extracting data for portal
            #print('no of items -',len(portal_data['ITEM_ID'].unique()))
            #print('no of users -',len(portal_data['USER_ID'].value_counts()))
            user_item_portal_rating[portal] = return_user_item_rating(portal_data)

        # Function to return n-top rated products for portal
        # threshold parameter is for minimum number of ratings a product has got
        def return_top_n_product(data,threshold=10,topn=10):
            topnprod = data.copy()
            topnprod = topnprod[['USER_ID','ITEM_ID','RATING']].groupby(['ITEM_ID']).agg({"RATING": ["count","mean"]})
            topnprod.columns=topnprod.columns.map('_'.join)
            topnprod=topnprod.reset_index()
            topnprod = topnprod[topnprod[('RATING_count')]>=threshold]
            topnprod.drop([('RATING_count')],axis=1,inplace=True)
            topnprod.rename(columns = {'RATING_mean':'RATING'}, inplace = True)
            return topnprod.sort_values('RATING',ascending=False).head(topn)

        noofrecommendation = 20
        top_trending_prod_for_portals = pd.DataFrame(columns=['ITEM_ID','RATING','PORTAL'])
        for portal in portals:    
            uniqueusers = len(user_item_portal_rating[portal].USER_ID.unique())
            if uniqueusers > 1000:
                threshold = 20
            elif uniqueusers > 500 and uniqueusers <= 1000:
                threshold = 10
            else:
                threshold = 5
            #print('threshold:',threshold, 'Portal:', portal,'unique user:',uniqueusers)
            top_prod_for_portal = return_top_n_product(user_item_portal_rating[portal],threshold,noofrecommendation)    
            top_prod_for_portal['PORTAL'] = portal
            #print(top_prod_for_portal.shape)
            top_trending_prod_for_portals = top_trending_prod_for_portals.append(top_prod_for_portal, ignore_index = True)

        logger.info("Exporting Top Trending recommendation to CSV file TOP_TRENDING_PROD_FOR_PORTALS")
        top_trending_prod_for_portals.to_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOP_TRENDING_PROD_FOR_PORTALS.CSV',index=False)
        logger.info("Finished Top Trending recommendation!")
        #########################Collaborating Filter###########################
        # Calculate score for every item using item item collaborating filtering 
        logger.info("Starting Collaborative Filter recommendation")
        recommended_items = dict()
        TopNNeighbours = pd.DataFrame(columns=['USER_ID','NEIGHBOURS','PORTAL'])
        predictions = dict()
        for portal in portals:
            #this dataframe has training data with rating and test data with NaN, we will replace the predicted rating for test data only
            df_p = user_item_portal_rating[portal]    
            df_p = pd.pivot_table(df_p, values='RATING', index='USER_ID', columns='ITEM_ID')

            reader = Reader(rating_scale=(1, 5))
            data = Dataset.load_from_df(user_item_portal_rating[portal], reader)
            #trainset, testset = train_test_split(data, test_size=1) # since surprise need test data in a particular format
            trainset = data.build_full_trainset()
            #looking for 20 neighbours for evaluation, min neighbour is 4
            algo = KNNWithMeans(k=20, min_k=4, sim_options={'name': 'pearson_baseline', 'user_based': True})
            algo.fit(trainset)
            print(portal)

            #Filling up the pivoted matrix    
            for i in df_p.index:
                raw_neighbours = []
                neighbours_filled = False
                for j in df_p.columns:
                    if df_p[j].loc[i]>0:
                        donothing = 1
                    else:                
                        pred = algo.predict(i, j, verbose=True)                
                        if pred[4]["was_impossible"] == False:
                            if pred[4]["actual_k"] >= 2: #assign only if atleast 2 neighbour found
                                df_p.at[i, j] = pred[3]
                                if(neighbours_filled == False):
                                    neighbors = algo.get_neighbors(algo.trainset.to_inner_uid(i), k=pred[4]["actual_k"])
                                    for u in neighbors:
                                        raw_neighbours.append(algo.trainset.to_raw_uid(u))                                
                                    neighbours_filled = True 
                                    new_row = {'USER_ID' : i,
                                            'NEIGHBOURS' : raw_neighbours,
                                            'PORTAL' : portal}
                                    TopNNeighbours = TopNNeighbours.append(new_row,ignore_index=True)
            recommended_items[portal] = df_p
        TopNNeighbours.reset_index(inplace=True,drop=True)
        logger.info("Exporting Exporting Top Neighbours for a user to CSV file TopNNeighbours")
        TopNNeighbours.to_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOPNNEIGHBOURS.CSV',index=False)

        noofrecommendation = 20
        top_col_filter_rec_prod_for_portals_users = pd.DataFrame(columns=['USER_ID','ITEM_ID','RATING','PORTAL'])
        for portal in portals:
            top_rec_prod_for_portals_user = recommended_items[portal].stack().reset_index(name='RATING')
            top_rec_prod_for_portals_user['PORTAL'] = portal
            for user in top_rec_prod_for_portals_user.USER_ID.unique().tolist():
                topitem = top_rec_prod_for_portals_user[top_rec_prod_for_portals_user['USER_ID']==user].sort_values(by=['RATING'],ascending=False)[:noofrecommendation]
                top_col_filter_rec_prod_for_portals_users = top_col_filter_rec_prod_for_portals_users.append(topitem, ignore_index = True)
        logger.info("Exporting Collaborative Filtering recommendation to CSV file TOP_COL_FILTER_REC_PROD_FOR_PORTALS_USERS")
        top_col_filter_rec_prod_for_portals_users.to_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOP_COL_FILTER_REC_PROD_FOR_PORTALS_USERS.CSV',index=False)
        logger.info("Finished Collaborative Filtering recommendation!")
        ############################Users own Top Rated Prod from Past###############################
        logger.info("Starting Top past rated recommendation")
        noofrecommendation = 20
        top_own_past_rated_prod_for_portals_users = pd.DataFrame(columns=['USER_ID','ITEM_ID','RATING','PORTAL'])
        for portal in portals:
            top_past_prod_for_portals_user = user_item_portal_rating[portal].copy()
            top_past_prod_for_portals_user['PORTAL'] = portal
            for user in top_past_prod_for_portals_user.USER_ID.unique().tolist():
                topitem = top_past_prod_for_portals_user[top_past_prod_for_portals_user['USER_ID']==user].sort_values(by=['RATING'],ascending=False)[:noofrecommendation]
                top_own_past_rated_prod_for_portals_users = top_own_past_rated_prod_for_portals_users.append(topitem, ignore_index = True)
        logger.info("Exporting Top past rated recommendation to CSV file TOP_OWN_PAST_RATED_PROD_FOR_PORTALS_USERS")
        top_own_past_rated_prod_for_portals_users.to_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOP_OWN_PAST_RATED_PROD_FOR_PORTALS_USERS.CSV',index=False)
        logger.info("Finished Top past rated recommendation!")
        ###############Hybrid from PastRated+CollabFilter+TopTrending###############
        logger.info("Starting Hybrid recommendation")
        noofrecommendation = 20
        thresholdfrompast = 3
        top_hybrid_rec_for_portals_users = pd.DataFrame(columns=['USER_ID','ITEM_ID','SORT_ORDER','PORTAL','MODEL_TYPE'])
        for portal in portals:    
            pastitem = top_own_past_rated_prod_for_portals_users[top_own_past_rated_prod_for_portals_users['PORTAL']==portal]
            recitem = top_col_filter_rec_prod_for_portals_users[top_col_filter_rec_prod_for_portals_users['PORTAL']==portal]
            trenditem = top_trending_prod_for_portals[top_trending_prod_for_portals['PORTAL']==portal].sort_values(by=['RATING'],ascending=False)['ITEM_ID'].tolist()
            for user in pastitem.USER_ID.unique().tolist():
                past_pop_items = []
                colrec_pop_items = []
                trend_pop_items = []
                trend_item = trenditem.copy()
                past = pastitem[pastitem['USER_ID']==user].sort_values(by=['RATING'],ascending=False)[:thresholdfrompast]['ITEM_ID'].tolist()
                rec = recitem[(recitem['USER_ID']==user) & (recitem['RATING']>1)].sort_values(by=['RATING'],ascending=False)['ITEM_ID'].tolist()
                if len(past)>0:
                    for element in past:
                        if element in rec:
                            rec.remove(element)
                        if element in trend_item:
                            trend_item.remove(element)
                if len(rec)>0:
                    for element in rec:
                        if element in trend_item:
                            trend_item.remove(element)
                past_pop_items = past.copy()
                if len(past_pop_items)<noofrecommendation:
                    colrec_pop_items = rec[:noofrecommendation-len(past_pop_items)]
                    if (len(past_pop_items)+len(colrec_pop_items))<noofrecommendation:
                        trend_pop_items = trend_item[:noofrecommendation-(len(past_pop_items)+len(colrec_pop_items))]
                        
                tempdf = pd.DataFrame(columns=['USER_ID','ITEM_ID','PORTAL','MODEL_TYPE'])
                if len(past_pop_items)>0:
                    new_row = {'USER_ID' : user,
                            'ITEM_ID' : past_pop_items,
                            'PORTAL' : portal,
                            'MODEL_TYPE' : 'PAST'}
                    tempdf = tempdf.append(new_row,ignore_index=True)
                if len(colrec_pop_items)>0:
                    new_row = {'USER_ID' : user,
                            'ITEM_ID' : colrec_pop_items,
                            'PORTAL' : portal,
                            'MODEL_TYPE' : 'COLREC'}
                    tempdf = tempdf.append(new_row,ignore_index=True)
                if len(trend_pop_items)>0:
                    new_row = {'USER_ID' : user,
                            'ITEM_ID' : trend_pop_items,
                            'PORTAL' : portal,
                            'MODEL_TYPE' : 'TREND'}
                    tempdf = tempdf.append(new_row,ignore_index=True)        
                tempdf = tempdf.explode('ITEM_ID')
                tempdf.reset_index(inplace=True,drop=True)
                tempdf['SORT_ORDER'] = tempdf.index+1
                top_hybrid_rec_for_portals_users = pd.concat([top_hybrid_rec_for_portals_users,tempdf])
        logger.info("Exporting Hybrid recommendation to CSV file TOP_HYBRID_REC_FOR_PORTALS_USERS")
        top_hybrid_rec_for_portals_users.to_csv('./data/jobid'+str(jobid)+'/predictionfiles/TOP_HYBRID_REC_FOR_PORTALS_USERS.CSV',index=False)
        logger.info("Finished Hybrid recommendation!")
        ###################################################################################
    except Exception as error:
        #print("Exception occurred: ", error)
        logger.error("Exception occurred", exc_info=True)
        raise
    else:
        #print("Prediction done successfully!!!")
        logger.info("Prediction done successfully!!!")
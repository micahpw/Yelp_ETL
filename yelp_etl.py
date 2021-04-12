import pandas as pd
import json
import numpy as np
import sqlite3
import os
import fnmatch
import json
import pandas as pd
import sys
import requests 
import zipfile
import argparse


def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

#Controller class for accessing data 
class YelpReviews():

    def __init__(self, root, pattern):
        super().__init__()
        self.root = root #Set root directory for data
        self.business_composition_pattern = pattern  #match pattern for json files to load
        self.sqlite_path = '/'.join([self.root,'user.sqlite']) #path to sqlite db    
        self.Business_Comp = None #objects to use as cache
        self.reviews = None


    #Load the 5 json files that contain the business hours compressed into 1 column/row value
    #If the Json format changes, we can change this method.
    def LoadJsonFile(self, filepath):
        data = json.load(open(filepath,'rb'))
        df = pd.DataFrame.from_records(data)

        #partial business comp files need to be transposed
        if len(df.columns.values) > 11:            
            return df.transpose()
        else:
            return df

    #Read the Business Composition in the JSON files.
    def GetBusinessComp(self):
        # Load data from memory if loaded previously
        if self.Business_Comp is not None:
            print('Using Cached business composition dataset')
            return self.Business_Comp
        else:
            print('Loading Business Composition data')
            #Load Json Files individually and combine
            frames = []
            for file in os.listdir(self.root):
                
                if fnmatch.fnmatch(file, self.business_composition_pattern):             
                    frames.append(self.LoadJsonFile('/'.join([self.root, file])))
                              
            
            self.Business_Comp = pd.concat(frames)
            return self.Business_Comp

    #Provide a basic connect, query, disconnect interface with the sqlite file.
    def QueryDB(self, sql):
        #Connect to database, load dataframe from query text, close database and return dataframe
        dbconn = sqlite3.connect(self.sqlite_path)
        df = pd.read_sql(sql, dbconn)
        dbconn.close()
        return df

    #Load reviews from the singular csv file.
    def GetReviews(self):
        #load reviews from cache if available
        if self.reviews is not None:
            return self.reviews
        else:
            self.reviews = pd.read_csv('/'.join([self.root,'reviews.csv']))
            return self.reviews    

    #Read in the data sets, compress the reviews into one column/row per business id.
    def CombineAllDatasets(self):
        Users = self.QueryDB('Select * from Users2;')
        Business_Attr = self.QueryDB('Select * from business_attributes;')
        
        reviews = self.GetReviews().set_index('Business - Id')
        
        #Loop through unique index values and compress reviews to list/dictionary
        print('Compressing Reviews... this may take a few minutes.')
        r_uid = reviews.index.unique()

        #Convert subsets of data into record/dictionary format based on index
        values = [str(reviews.loc[bus_id].to_dict('records')) 
        if type(reviews.loc[bus_id]['Review - Id']) != str 
        else str(reviews.loc[bus_id].to_frame().transpose().to_dict('records')) 
        for bus_id in r_uid]

        #Create compressed Reviews data frame
        compressed_reviews = pd.DataFrame(values, index=r_uid, columns=['Compressed_Reviews'])
        print('Finished compressing reviews')

        Business_Comp = self.GetBusinessComp()  

        merge1 = pd.merge(Business_Attr, Business_Comp, left_on='Business - Id', right_on='business_id')

        df_all = pd.merge(merge1.set_index('Business - Id'), compressed_reviews, left_index=True, right_index=True)

        return df_all


    def MeanReviewsByBusiness(self):
        df = self.GetReviews()        
        reviews = df[['Business - Id', 'Review - Stars', 'Review - Votes Cool', 'Review - Votes Funny', 'Review - Votes Useful']]    
        business_comp = self.GetBusinessComp()
        return reviews.groupby('Business - Id').mean()        

    #Generate the top n zip codes. Currently hardcoded to be the top 5.
    def MeanReviewsByZipCode(self, n=5):

        bus_comp = self.GetBusinessComp()
        bus_comp['ZipCode'] = bus_comp['Business - Address'].str.split(' ').apply(lambda x: x[-1]) #Split the address by spaces and get last object.

        reviews = self.GetReviews()

        #Get top n dense business zip codes
        z = bus_comp.groupby('ZipCode')['business_id'].count().sort_values(ascending=False).head(n)        
        bus_comp = bus_comp.set_index('ZipCode').loc[z.index.values].reset_index()

        df = pd.merge(bus_comp, reviews, left_on='business_id', right_on='Business - Id')                      

        return df.groupby('ZipCode')[['Review - Stars', 'Review - Votes Cool', 'Review - Votes Funny', 'Review - Votes Useful']].mean()

    #List the top n active reviews, option to expand to a larger set of reviewers.
    def MostActiveReviewers(self, n=10):
        Users2 = self.QueryDB('Select * from Users2;')   
        return Users2.groupby(['User - Id', 'User - Name'])['Review - Id'].count().sort_values(ascending = False).head(n)
        
        


if __name__ == "__main__":

    #Create argument parser
    parser = argparse.ArgumentParser(description='Extract Yelp Data and move to S3')    
    parser.add_argument('--s3uri', type=str)
    parser.add_argument('--data_url', type=str)

    args = parser.parse_args()


    ## Download file from URL and unzip
    print('Downloading data from {}'.format(args.data_url))
    url = args.data_url    
    
    data_path = './data'
    path_to_zip_file = data_path + '/Yelp.zip'

    download_url(url, path_to_zip_file, chunk_size=1024)
    
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(data_path)


    # Create Data Abstraction layer object
    YR = YelpReviews(data_path+'/output', 'business_composition_final*')
    
    # Run through the various tasks for this project.
    print('\n Joining Datasets into summary data set \n')           
    df = YR.CombineAllDatasets()
    print(df.head())


    print('\n Calculating Mean reviews by Business \n')
    br = YR.MeanReviewsByBusiness()
    print(br.head())

    print('\n Calculating Mean Reviews by the Top 5 Densest Zip Codes \n')
    zr = YR.MeanReviewsByZipCode()
    print(zr.head())

    print('\n Retrieving top 10 most active reviewers \n')
    ur = YR.MostActiveReviewers()
    print(ur.head())


    ### Upload Extracted data for Users, Reviews, Business Attributes and Business Compositions
    print('\n Uploading extracted data sets into S3 directory {} \n'.format(args.s3uri))
    uri = args.s3uri

    #Get full dataframes to upload.
    bc = YR.GetBusinessComp()
    r = YR.GetReviews()

    Users = YR.QueryDB('Select * from Users2;')
    Business_Attr = YR.QueryDB('Select * from business_attributes;')

    #Upload to s3
    print('Uploading to '+uri+'{}'.format('YelpBusinessComposition.csv'))
    bc.to_csv(uri+'{}'.format('YelpBusinessComposition.csv'))
    
    print('Uploading to '+uri+'{}'.format('YelpReviews.csv'))
    r.to_csv(uri+'{}'.format('YelpReviews.csv'))
    
    print('Uploading to '+uri+'{}'.format('YelpUsers.csv'))
    Users.to_csv(uri+'{}'.format('YelpUsers.csv'))
    
    print('Uploading to '+uri+'{}'.format('YelpBusinessAttributes.csv'))
    Business_Attr.to_csv(uri+'{}'.format('YelpBusinessAttributes.csv'))
    
    print('Finished uploading data to s3. Exiting program.')
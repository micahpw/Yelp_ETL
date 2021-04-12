# Yelp_ETL

*Description: A simple ETL pipeline for Yelp Data.*

## Install Requirements

To get started, make sure to run the following command and install the necessary python modules. We wil primarily use the external libraries of requests, pandas and s3fs.

`pip install -r requirements.txt`


## Running the program

To run the program, you will need to use the `aws configure` command and input your **access** and **private key** information

`python yelp_etl.py --s3uri=<path/to/s3/bucket> --data_url=<url/to/data>`

for my current example I used

`python yelp_etl.py --s3uri=s3://mwebb-slalom-yelp/data/ --data_url=https://dataengineeringexercise.s3-us-west-1.amazonaws.com/Yelp_dataengineering_dataset.zip`



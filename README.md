# Greenflag

Data-Engineer-Test

Tools/Technologies used: 
Python - For the data transformations
Amazon S3-To store the  transformed data in parquet form
AWS Athena-To query the data and get the answers

weather.py
----------

Script that takes input data(2 csv files) ,merge them and convert it to parquet format and uploads the file  to s3 bucket created by using boto3 library


test_weather.py
----------------

Test module that independently tests small units of code in weather.py


Athena.txt
-----------

 This has the script to create an external table and point to the data in s3 that we have uploaded earlier
 Script to query the data  to get the below results:
    Which date was the hottest day
    Temperature on that day
    Region of the hottest day


Results  and  evidence
----------------------

Athena.png:screenshot taken from athena which  provides the answers for the above queations
   date of the  hottest day:2016-03-17
   temperature: 15.8
   region:Highland & Eian.Siar

filepath.py
-----------
this  script has all  required environment variables creation



Assumptions or additional steps:
---------------------------------
I have configured AWS using Access key ,Secret key ,region as environment  variables and used in my python script with getenv function
I have configured the file path as environment variables and referred in the script
Imputed the null values  with 0 or empty string depending on data type
Converted the string  type of observation date to datetime format


Running Instructions:
---------------------
First you need to set the file path and configure aws ccess key ,secret key and region as environment variables and run the python script(if you run without setting the environment variables it will throw an error).
I have created a  filepath.py which  has  the  environment variable script(i have masked aws key values for security)

run the  python script with  the  bucket name (my-bucket for  example )as  the input parameter(this  creates s3 bucket and stores the parquet file in it)

python3 weather.py my-bucket

To run the test suite
------------------------
python3 -m unittest test_weather.py













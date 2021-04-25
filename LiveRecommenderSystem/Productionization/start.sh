#!/bin/bash
bucket_name="aiml-recommendersystem-data"
echo "copying file from s3 to data"
cd data
aws s3 sync s3://${bucket_name}/${ENV}/ .

cd ..
python3 ./PythonScripts/Main.py

date=$(date '+%Y-%m-%d')

echo "copying file"
cd data
aws s3 sync . s3://${bucket_name}/${ENV}

#/${date} --exclude "jobid*/*.log"
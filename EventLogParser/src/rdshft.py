import gzip
import json
import boto3
from io import BytesIO
from pyspark.sql import SQLContext
import psycopg2

sqlContext = SQLContext(sc)

jsonList = []

def publish_sns_message(payload,message_structure,subject=None):
    sns_client = boto3.client('sns',region_name='us-east-1')
    topic_arn = 'arn:aws:sns:us-east-1:749859013549:octa-devl-edl-test'
    response = sns_client.publish(TargetArn=topic_arn, Message=payload, MessageStructure=message_structure,Subject=subject)
    print(response)
    return response

def file_cleanse(s3Bucket,prefix):
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(s3Bucket)
	# Reading the raw .gz file from s3 and processing it
	for obj in bucket.objects.filter(Prefix=prefix).all():
		buffer = BytesIO(obj.get()['Body'].read())
		gzipfile = gzip.GzipFile(fileobj=buffer)
	for line in gzipfile:
		json_object = json.loads(line)       
		jsonList.append(json_object)
		
	df=sqlContext.createDataFrame(jsonList)
	srcTempJsonTbl = 'temp_json'
	df.registerTempTable(srcTempJsonTbl)
	srcDataFrame = sqlContext.sql('select  year,season,count(distinct team),now() from temp_json where medal is not null group by year,season')
	# Step - 1
	srcDataFrame.show()
	# Writing the data to s3. This file will be read using external schema - test_poc
	srcDataFrame.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").mode("overwrite").save("s3://BUCKET-NAME/tmp/test/output/")

def rdsht_write(redshiftDatabase,redshiftUser,redshiftPasswd,redshiftPort,redshiftEndpoint):
	redshiftClient = psycopg2.connect(
									dbname=redshiftDatabase,
									user=redshiftUser,
									password=redshiftPasswd,
									port=redshiftPort,
									host=redshiftEndpoint)
									
	cur = redshiftClient.cursor()
	# Step - 2. Reading from glue catalog/Redshift Spectrum and writing to Redshift physical table
	try:
		cur.execute("UPDATE redshift_schema.assignment_poc SET (countries_with_medals, ins_upd_ts) = (SELECT test_poc.countries_with_medals, test_poc.ins_upd_ts FROM glue_schema.test_poc WHERE test_poc.year = assignment_poc.year and test_poc.season = assignment_poc.season and test_poc.countries_with_medals != assignment_poc.countries_with_medals) WHERE EXISTS ( SELECT * FROM glue_schema.test_poc WHERE test_poc.year = assignment_poc.year and test_poc.season = assignment_poc.season and test_poc.countries_with_medals != assignment_poc.countries_with_medals);COMMIT;INSERT INTO redshift_schema.assignment_poc WITH SRC AS (SELECT * from glue_schema.test_poc) select year,season,countries_with_medals,ins_upd_ts from SRC where (year,season) not in (select year,season from redshift_schema.assignment_poc);")
		redshiftClient.commit()
		cur.execute("select max(ins_upd_ts) from redshift_schema.assignment_poc;")
		a = cur.fetchall()[0][0]
		cur.close() 
		print('redshift executed successfully')
		status = 'SUCCESS'
		last_upd_ts = datetime.datetime.strptime(a,'%Y-%m-%d %H:%M:%S.%f')
        latency = datetime.datetime.now()-last_upd_ts
		
		if latency.seconds/3600 > 3:
			subject = 'latency exceeded by {0} hrs'.format(latency.seconds/3600)
            msg = 'latency exceeded by {0} hrs. Last record insert/update time is at {1}. Please verify'.format(latency.seconds/3600,last_upd_ts)

            # Step - 3. Puts a message in SNS Topic
            publish_sns_message(msg,'string',subject)
		else:
            msg = 'No latency. Last updated time is {0} and latency is {1} hrs'.format(last_upd_ts,latency)
        print(msg)
	except:
		# raise 
		print('redshift execution failed')
		status = 'FAILED'
	return status

file_cleanse('BUCKET-NAME','tmp/test/athlete_events_2006_2016.jsonl.gz')

redshiftDatabase = "redshift_schema"
redshiftPort = 5439
redshiftEndpoint = "octa-rs01.xxxxxxxxxxxx.us-east-1.redshift.amazonaws.com"
redshiftUser = "test_user"
redshiftPasswd = "testpwd@123"

rdsht_write(redshiftDatabase,redshiftUser,redshiftPasswd,redshiftPort,redshiftEndpoint)
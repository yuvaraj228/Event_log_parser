import gzip
import json
import pandas as pd
import pandasql as pds
import sqlite3 as sql
import datetime
import boto3

# ct stores current time
ct = datetime.datetime.now()
jsonList = []

# Publish e-mail notification if latency > 3hrs
def publish_sns_message(payload,message_structure,subject=None):
    sns_client = boto3.client('sns',region_name='us-east-1')
    topic_arn = 'arn:aws:sns:us-east-1:749859013549:octa-devl-edl-test'
    response = sns_client.publish(TargetArn=topic_arn, Message=payload, MessageStructure=message_structure,Subject=subject)
    print(response)
    return response

# Input file cleansing/standardization
def file_cleanse(input_file):
    with gzip.open(input_file, 'r') as f:
        for line in f:
           jsonDict = json.loads(line)
           jsonList.append(jsonDict)

    print('Json converted')

    # Flattening the json to read the data
    i=pd.json_normalize(jsonList)
    i.insert(0,'ins_upd_ts',ct)
    query='select year,season,count(distinct team) countries_with_medals,ins_upd_ts from i where medal is not null group by year,season'
    op_df=pds.sqldf(query)
    # Step - 1
    print(op_df)    

    return op_df

# writing to db
def db_connect(db,df,lag_notif):
    conn = sql.connect(db)

    with conn:
        cur = conn.cursor()
        # writing the data to a temp table. Truncate and load everytime
		df.to_sql(name='test_poc', con=conn, if_exists='replace', index=False)
        
		# Upsert - Updates already existing records if there's a change. Inserts new and ignores if there's no change		
        cur.executescript("UPDATE assignment_poc SET (countries_with_medals, ins_upd_ts) = (SELECT test_poc.countries_with_medals, test_poc.ins_upd_ts FROM test_poc WHERE test_poc.year = assignment_poc.year and test_poc.season = assignment_poc.season and test_poc.countries_with_medals != assignment_poc.countries_with_medals) WHERE EXISTS ( SELECT * FROM test_poc WHERE test_poc.year = assignment_poc.year and test_poc.season = assignment_poc.season and test_poc.countries_with_medals != assignment_poc.countries_with_medals);INSERT INTO assignment_poc WITH SRC AS (SELECT * from test_poc) select year,season,countries_with_medals,ins_upd_ts from SRC where (year,season) not in (select year,season from assignment_poc);")
        
		# Exception handling when there's nothing to commit
		try:
            cur.execute("COMMIT;")
        except:
            pass
			
        cur.execute('select max(ins_upd_ts) from assignment_poc;')
		
		# latency calculation based on last upsert timestamp and current timestamp
        last_upd_ts = datetime.datetime.strptime(cur.fetchall()[0][0],'%Y-%m-%d %H:%M:%S.%f')
        latency = datetime.datetime.now()-last_upd_ts
        print(latency.seconds)
        if latency.seconds/3600 > lag_notif:
            subject = 'Latency exceeded by {0} hrs'.format(latency.seconds/3600)
            msg = 'Latency exceeded by {0} hrs. Last record insert/update time is at {1}. Please verify'.format(latency.seconds/3600,last_upd_ts)

            # Puts a message in SNS Topic
            publish_sns_message(msg,'string',subject)
        else:
            msg = 'No latency. Last updated time is {0} and latency is {1} hrs'.format(last_upd_ts,latency)
        print(msg)
	# Closing DB Connection
    if conn:
        conn.close()

cleansed_df = file_cleanse('athlete_events_2006_2016.jsonl.gz')

db = "test.db"
lag_notif = 3
db_connect(db,cleansed_df,lag_notif)

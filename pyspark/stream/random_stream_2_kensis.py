import time
import json
import boto3
import random
import calendar
from datetime import datetime
from pprint import pprint
import os 

KINESIS_STREAM_NAME =  os.environ['KINESIS_STREAM_NAME'] 
REGION_NAME = os.environ['REGION_NAME'] 


def write_to_stream(event_id, event, region_name, stream_name):
    """Write streaming event to specified Kinesis Stream within specified region.
    Parameters
    ----------
    event_id: str
        The unique identifer for the event which will be needed in partitioning.
    event: dict
        The actual payload including all the details of the event.
    region_name: str
        AWS region identifier, e.g., "ap-northeast-1".
    stream_name: str
        Kinesis Stream name to write.
    Returns
    -------
    res: Response returned by `put_record` func defined in boto3.client('kinesis')
    """
    client = boto3.client('kinesis', region_name=region_name)
    res = client.put_record(
        StreamName=stream_name,
        Data=json.dumps(event) + '\n',
        PartitionKey=event_id
    )
    return res


if __name__ == '__main__':
    # simulate streaming data generation
    while True:
        event = {
            "event_id": str(random.randint(1, 100000)),
            "event_type": random.choice(['open_app', 'close_app', 'make_comments']),
            "user_id" : str(random.randint(1, 30)), 
            "timestamp": calendar.timegm(datetime.utcnow().timetuple())
        }
        pprint(event)

        # send to Kinesis Stream
        event_id = event['event_id']
        write_to_stream(event_id, event, REGION_NAME, KINESIS_STREAM_NAME)
        time.sleep(5)
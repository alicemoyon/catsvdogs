import base64
import json


def lambda_handler(event, context):
    output = []
    for record in event['records']:
        # decode the record data
        payload = base64.b64decode(record['data']).decode()
        # convert decoded string into json
        payload = json.loads(payload)
        print(type(payload))
        # clean up the record
        cleaned_payload = dict(tweet_id=payload['data']['id'],
                               retweets=payload['data']['public_metrics']['retweet_count'],
                               replies=payload['data']['public_metrics']['reply_count'],
                               likes=payload['data']['public_metrics']['like_count'],
                               category=payload['matching_rules'][0]['tag'],
                               timestamp=record['approximateArrivalTimestamp']
                              )
        # convert dictionary to json string and re-encode the payload
        new_payload = base64.b64encode(json.dumps(cleaned_payload).encode())
        output.append({
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': new_payload
        })
    return {'records': output}


import requests
import json
import configparser
import boto3
import logging

# Script adapted from:
# https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/master/Filtered-Stream/filtered_stream.py

query_parameters = "tweet.fields=id,public_metrics"

config = configparser.ConfigParser(interpolation=None)
config.read_file(open('credentials.cfg'))

AWS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
AWS_SECRET = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
FIREHOSE_ARN = config.get('AWS', 'FIREHOSE_ROLE_ARN')

firehose = boto3.client('firehose')


def create_headers(bearer_token):
    """
    Creates the dictionary containing the authorisation (bearer) token for Twitter API
    authentication
    """
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def get_rules(headers):
    """
    Prints and returns any existing filtering rules for the stream or throws an exception if no rules exist
    """
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, rules):
    """
    Deletes the existing filtering rules if any exist
    """
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(headers):
    """
    - Creates a dictionary of filtering rules
    - Connects to the Twitter API with the rules as input to set the filtering rules for the stream
    """
    filter_rules = [
            {"value": "(#cat OR #kitty OR #cats OR #kitten) has:images",
             "tag": "cats with images"},
            {"value": "(#dog OR #doggo OR #doggy OR #puppy) has:images",
             "tag": "dogs with images"},
            {"value": "(#cat OR #kitty OR #cats OR #kitten) has:videos",
             "tag": "cats with videos"},
            {"value": "(#dog OR #doggo OR #doggy OR #puppy) has:videos",
             "tag": "dogs with videos"}
    ]
    payload = {"add": filter_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(headers):
    """
    Opens a persistent connection to the Twitter streaming API using a set of query parameters.
     The query parameters determine which fields from a Tweet will be returned from the API stream
    """
    # print("The URI request string is: https://api.twitter.com/2/tweets/search/stream?" + query_parameters)
    while True: # necessary to ensure any disconnection is reconnected automatically without intervention and restart
        # of the stream
        try:
            response = requests.get(
                "https://api.twitter.com/2/tweets/search/stream?" + query_parameters,
                headers=headers,
                stream=True,
                timeout=(5, 60)
            )
            print(response.status_code)
            if response.status_code != 200:
                raise Exception(
                    "Cannot get stream (HTTP {}): {}".format(
                        response.status_code, response.text
                    )
                )

            for record in response.iter_lines():
                if record:
                    try:
                        r = firehose.put_record(
                            DeliveryStreamName='cvd-twitter-es-delivery-stream',
                            Record={
                                'Data': record
                            }
                        )
                        # print("Record ", r['RecordId'], "added to Firehose stream")
                    except Exception as e:
                        logging.exception(e)
                        continue

        except Exception as e:
            logging.exception(e)
            continue


def main():
    # config = configparser.ConfigParser()
    # config.read_file(open('credentials.cfg'))
    bearer_token = config.get('TWITTER', 'BEARER_TOKEN')
    headers = create_headers(bearer_token)
    rules = get_rules(headers)
    delete_all_rules(headers, rules)
    set_rules(headers)
    get_stream(headers)


if __name__ == "__main__":
    main()

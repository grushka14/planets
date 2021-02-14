import json
import base64
import boto3
import uuid
import time


def kinesis_kfh_output_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = "formatted-planet-scores"

    print(event)
    output = list(map(lambda x: json.loads(base64.b64decode(x['data'])), event['records']))
    print(output)
    for planet in output:
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{planet['planet']} - {uuid.uuid4()}",
            Body=json.dumps(planet),
            ACL='public-read'
        )


def get_average_handler(event, context):
    client = boto3.client('athena')

    try:
        start_date = str(event['queryStringParameters']['from'])
        end_date = str(event['queryStringParameters']['to'])
    except Exception as e:
        print(f"Exception at get_average_handler: {e}")
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "invalid parameters"}),
        }
    if type(start_date) != str or type(end_date) != str or start_date == "" or end_date == "":
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "invalid parameters"}),
        }
    try:
        query = f"select planets.formated_planets_scores.planet, avg(planets.formated_planets_scores.score) as avg," \
                f" sum(planets.formated_planets_scores.events) as events from planets.formated_planets_scores" \
                f" where ts between from_iso8601_timestamp('{start_date}T20:29:00.000Z') and " \
                f"from_iso8601_timestamp('{end_date}T20:35:00.000Z') " \
                f"group by planets.formated_planets_scores.planet;"
        response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': "s3://athena-outpul-98711258/",
        })
        QueryExecutionId = response['QueryExecutionId']
    except Exception as e:
        print(f"exception at query: {e}")
        return {
            "statusCode": 500
        }
    try:
        time.sleep(5)
        repeat = 0
        status = 'RUNNING'
        response_data = []
        while repeat <= 5 and status != 'FINISHED':
            try:
                repeat += 1
                res = client.get_query_execution(QueryExecutionId=QueryExecutionId)
                if res['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                    res = client.get_query_results(QueryExecutionId=QueryExecutionId)
                    for row in res['ResultSet']['Rows'][1:]:
                        response_data.append({
                            "type": row['Data'][0]['VarCharValue'],
                            "value": row['Data'][1]['VarCharValue'],
                            "planetsCount": row['Data'][2]['VarCharValue']
                        })
                    status = 'FINISHED'
            except Exception as e:
                print(e)
                print("trying again in 5 seconds")
                time.sleep(5)
    except Exception as e:
        print(f"exception at get res: {e}")
    return {
        "statusCode": 200,
        "body": json.dumps(response_data),
    }


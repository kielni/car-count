# Traffic data collection and alerting

An AWS Lambda function runs at 10, 25, 40, and 55 past the hour.
It gets data from SNAPS for the 15 minute period from 20 minutes ago to 5 minutes ago,
and sends to a Google Sheet.

At 4pm and 5pm, if at least 150 entries have been record, make a prediction for the whole
day using linear regression with data from Aug 2018 - Sep 2019.
If predicted is greater than 400, send alert via SNS.

The code is packaged and deployed to AWS with [serverless](https://www.serverless.com/).

The AWS Lambda has these environment variables:

  - ALERT_ARN - where to send alert (`arn:sws:sns:...`)
  - GOOGLE_SHEET_ID - identifier for Google Sheet
  - STATIONS: `{"entry": "entryStationName", "exit": "exitStationName"}`

Sensitive parameters are stored encrypted in
the [AWS Systems Manager Parameter Store](https://us-west-2.console.aws.amazon.com/systems-manager/parameters/?region=us-west-2&tab=Table):

  - hillbrook-traffic-service-account - key for accessing Google Sheet
  - SNAPS_USERNAME - `userName` param for SNAPS API
  - SNAPS_PASSWORD - `password` param for SNAPS API

## prerequisites

Set up a python3 3.7 virtual env:
  - install [pyenv](https://github.com/pyenv/pyenv)
  - install [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)

```
pyenv install 3.7.1
pyenv virtualenv 3.7.1 aws-lambda-37
pyenv activate aws-lambda-37
pip install -r dev-requirements.txt
pip install -r requirements.txt
```

Install serverless and Python dependencies packaging plugin:
  - [serverless](https://serverless.com/framework/docs/providers/aws/guide/quick-start/)
  - install `serverless-python-requirement`: `sls plugin install -n serverless-python-requirements`

Set up access for the AWS Lambda to the Google sheet. [Python with Google Sheets Service Account](https://medium.com/@denisluiz/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e) has
an excellent description.

  - Create a project in the [Google console](https://console.developers.google.com/projectselector/apis/library?pli=1&supportedpurview=project)
  - Enable the Google Drive API
  - Create a Service account key from Create credentials
  - Generate a JSON key and save it
  - Add the JSON key to the `hillbrook-traffic-service-account` parameter in [AWS Systems Manager Parameter Store](https://us-west-2.console.aws.amazon.com/systems-manager/parameters/?region=us-west-2&tab=Table)
  - Create a Google sheet and share it with the `client_email` from the JSON key

## deploy

everything (if config changes): `sls deploy`

function changes only: `serverless deploy function -f collect-sheet`

test: `serverless invoke -f collect-sheet --log`

## cost

### AWS Lambda

for running the function that collects and saves data

68 invocations / day
average invocation time of 7 seconds = 476 seconds/day
128MB configured = 59.5 GB/s/day or about 1,785 GB-seconds/month

[Free tier](https://aws.amazon.com/lambda/pricing/) includes 400,000 GB-seconds/month

### AWS SNS

for sending email and text message alerts

up to 3 alerts/day, 1-2 times/week

#### email

3 subscribers

[first 1,000 email deliveries are free]((https://aws.amazon.com/sns/pricing/))

#### SMS

3 subscribers

outbound messages are [$0.00645/each](https://aws.amazon.com/sns/sms-pricing/)

high estimate: 3 subscribers * 3 alerts per day * 2 alerts/week * 4 weeks/month = $0.46 / month


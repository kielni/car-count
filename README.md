# Traffic data collection and alerting

AWS Lambda function runs at 10, 25, 40, and 55 past the hour to
get data from SNAPS for 15 minute period from 20 minutes ago to 5 minutes ago
and send to [Keen.io](https://keen.io/) `traffic` collection. 

At 4pm and 5pm, get prediction for the whole day using linear regression over
data from Aug 2018 - Sep 2019. If predicted is greater than 400, send alert via SNS.

Environment

  - STATIONS: `{"entry": "entryStationName", "exit": "exitStationName"}`
  - SNAPS_USERNAME: SNAPS URL `userName` param
  - SNAPS_PASSWORD: SNAPS URL `password` param
  - KEEN_PROJECT_ID
  - KEEN_WRITE_KEY
  - ALERT_ARN - where to send alert (`arn:sws:sns:...`)

## prerequisites

Set up a python3 3.7 virtual env:
  - install [pyenv](https://github.com/pyenv/pyenv)
  - install [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)

```
pyenv install 3.7.1
pyenv virtualenv 3.7.1 aws-lambda-37
pyenv activate aws-lambda-37
pip install -r requirements.txt
```

Install serverless and Python dependencies packaging plugin:
  - [serverless](https://serverless.com/framework/docs/providers/aws/guide/quick-start/)
  - install `serverless-python-requirement`: `sls plugin install -n serverless-python-requirements`

Share Google Sheet with service account (user@domain.iam.gserviceaccount.com)

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


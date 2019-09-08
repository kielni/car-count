from datetime import datetime, timedelta
import json
import os
import time

import boto3
from dateutil import tz
import keen
import requests
import xmltodict


COLLECTION = 'traffic'

"""
    Get data from SNAPS for 15 minute period from 20 minutes ago to 5 minutes ago,
    then send to Keen.io traffic collection.

    environment
        STATIONS {"entry": "entryStationName", "exit": "exitStationName"}
        EXTRA_CARS must have default value {"football": 30, "default": 10}
        CALENDAR_URL url to .ics format calendar
        SNAPS_USERNAME
        SNAPS_PASSWORD
        KEEN_PROJECT_ID
        KEEN_WRITE_KEY
        ALERT_ARN
"""


SNAPS_URL = 'https://satts11.sensysnetworks.net/snaps/dataservice/stats.xml?userName={username}&password={password}&startTime={start_ts}&period={period}&locationGroup={station}'


def get_counts(station, start_ts, period):
    url = SNAPS_URL.format(username=os.environ['SNAPS_USERNAME'],
                           password=os.environ['SNAPS_PASSWORD'],
                           start_ts=start_ts, period=period,
                           station=station)
    resp = requests.get(url, verify=False)
    data = xmltodict.parse(resp.text)
    if 'statistics' not in data:
        print('error: bad data: %s' % data)
        return {}

    values = {}
    for lane in data['statistics']['approach']['lanes']['lane']:
        name = lane['@name']
        values[name] = int(lane['stat']['@volume'])
    print('station=%s startTime=%s period=%s values=%s' % (station, start_ts, period, values))
    return values



def _prediction(hour, observed):
    """
    linear regression with Aug 2018-Sep 2019 data
    4pm: coeff=[1.01678193] intercept=44.664357451929504
        score=0.7009697101722595
    as of 5pm: coeff=[1.00879315] intercept=16.994550748946494
        score=0.8602885443570323
    """
    # (coef, intercept)
    calc = {
        16: (1.01678193, 44.664357451929504),
        17: (1.00879315, 16.994550748946494)
    }
    if hour not in calc:
        return 0
    return int(round(float(observed) * calc[hour][0] + calc[hour][1]))


def collect(event, context):
    keen.project_id = os.environ['KEEN_PROJECT_ID']
    keen.write_key = os.environ['KEEN_WRITE_KEY']
    keen.read_key = os.environ['KEEN_READ_KEY']
    print('event=', event)

    # the same rule can be triggered more than once for a single event or scheduled time
    # https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/CWE_Troubleshooting.html#RuleTriggeredMoreThanOnce
    if keen.count(COLLECTION, timeframe='this_10_minutes'):
        print('ERROR: duplicate invocation; event=', event)
        return

    # UTC
    start = datetime.now() - timedelta(minutes=20)
    start.replace(second=0, microsecond=0)
    start_ts = int(time.mktime(start.timetuple()))
    # 15 minutes in seconds
    period = 15 * 60
    # start of Pacfic time day
    now_pt = datetime.now(tz.gettz('America/Los_Angeles'))
    day_start = datetime.now(tz.gettz('America/Los_Angeles'))
    day_start = day_start.replace(hour=0, minute=0, second=0)
    day_start_ts = int(time.mktime(day_start.astimezone(tz.gettz('UTC')).timetuple()))
    # seconds between start of day and 5 minutes ago
    day_period = now_pt.hour*3600 + now_pt.minute*60 + now_pt.second - 5*60
    values = {
        'startTime': start_ts,
        'period': period,
        'time': datetime.now(tz.gettz('America/Los_Angeles')).strftime('%H%M%S'),
    }

    # predictions M-F 4pm and 5pm
    alert = {}
    predict = now_pt.hour in [16, 17] and now_pt.weekday() < 5 and now_pt.minute < 15
    alert_key = 'EntryA'

    # get data from SNAPS
    stations = json.loads(os.environ['STATIONS'])
    for station_type in stations:
        # 15 minutes of data starting 20 minutes ago
        values[station_type] = get_counts(stations[station_type], start_ts, period)
        if not predict:
            print('skipping prediction: hour=%s weekday=%s minute=%s' % (
                now_pt.hour, now_pt.weekday(), now_pt.minute))
            continue
        # get full day counts up to 5 minutes ago
        day = get_counts(stations[station_type], day_start_ts, day_period)
        print('hour=%s start=%s period=%s full day=%s' % (
            now_pt.hour, day_start_ts, day_period, day))
        if alert_key not in day:
            print('no data for %s: %s', (alert_key, day))
            continue
        if day[alert_key] < 150:
            print('actual %s; not a school day' % day[alert_key])
            continue
        predicted = _prediction(now_pt.hour, day[alert_key])
        if not predicted:
            print('no prediction available for %s %s' % (now_pt.hour, day))
            continue
        alert = {
            'key': alert_key,
            'actual': day[alert_key],
            'predicted': predicted
        }
        values[station_type]['prediction'] = {
            'actual': day[alert_key],
            'predicted': predicted
        }
    # send to Keen
    print('values=%s' % values)
    keen.add_event(COLLECTION, values)

    # send alert if > 400
    if alert.get('predicted', 0) > 400:
        subject = 'WARNING: high car count: %s predicted as of %s' % (
            alert['predicted'], alert['key'])
        message = 'WARNING: %s cars measured at %s as of %s. %s cars predicted.' % (
            alert['actual'], alert['key'], now_pt.strftime('%-I:%M%p'),
            alert['predicted'])
        print(boto3.client('sns').publish(
            TopicArn=os.environ['ALERT_ARN'],
            Message=message, Subject=subject))

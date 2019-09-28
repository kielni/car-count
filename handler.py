from datetime import datetime, timedelta
import json
import os
import time

import boto3
from dateutil import tz
from dateutil import parser as date_parser
import gspread
import keen
from oauth2client.service_account import ServiceAccountCredentials
import requests
import xmltodict


COLLECTION = 'traffic'

"""
    Get data from SNAPS for 15 minute period from 20 minutes ago to 5 minutes ago,
    then send to a Google Sheet.

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



def _prediction(now_pt: int, observed: int) -> int:
    """
    4pm and 5pm:
        linear regression with Aug 2018-Sep 2019 data
        4pm: coeff=[1.01649594] intercept=44.91931545628796 score=0.6930983959871696
        5pm: coeff=[1.00662224] intercept=17.955353264565133 score=0.8480492171160054
    1pm
    """
    hour = now_pt.hour
    if hour in [16, 17]:
    # (coef, intercept)
        calc = {
            16: (1.01649594, 44.91931545628796),
            17: (1.00662224, 17.955353264565133)
        }
        return int(round(float(observed) * calc[hour][0] + calc[hour][1]))
    # 1pm Mon-Wed: day: threshold
    by_day = {
        0: 222,
        1: 220,
        2: 211,
    }
    weekday = now_pt.weekday()
    if weekday not in by_day:
        return 0
    # high or not; not a specific prediction
    return 401 if observed >= by_day[weekday] else 0


def send_alert(alert: dict, now_pt: datetime):
    # send alert if > 400
    if alert.get('predicted', 0) <= 400:
        return
    # early prediction: high or not
    if now_pt.hour == 13:
        subject = 'WARNING: high car count predicted as of %s' % (
            alert['key'])
        message = 'WARNING: %s cars measured at %s as of %s. Over 400 entries predicted.' % (
            alert['actual'], alert['key'], now_pt.strftime('%-I:%M%p'))
    else:
        subject = 'WARNING: high car count: %s predicted as of %s' % (
            alert['predicted'], alert['key'])
        message = 'WARNING: %s cars measured at %s as of %s. %s cars predicted.' % (
            alert['actual'], alert['key'], now_pt.strftime('%-I:%M%p'),
            alert['predicted'])
    print(boto3.client('sns').publish(
        TopicArn=os.environ['ALERT_ARN'],
        Message=message, Subject=subject))


def update_sheet(values: dict, now_pt: datetime):
    # setup sheet
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    client = gspread.authorize(credentials)
    ss = client.open_by_key(os.environ['GOOGLE_SHEET_ID'])

    # 4 per hour starting at 7am, plus 2 for date and total
    col = (now_pt.hour - 7) * 4 + int(now_pt.minute / 15) + 2
    # sheets: EntryA, EntryB, prediction
    worksheets = {'EntryA': 0, 'EntryB': 1, 'prediction': 2}
    mdy = now_pt.strftime('%-m/%-d/%y')
    for key in ['EntryA', 'EntryB']:
        val = values['entry'][key]
        sheet = ss.get_worksheet(worksheets[key])
        latest = date_parser.parse(sheet.range('A2:A2')[0].value).date()
        if now_pt.date() == latest:
            # row for this day already exists; update cell
            print('%s: updating row=2 col=%s: %s' % (sheet.title, col, val))
            sheet.update_acell('%s2' % chr(65 + col), val)
        else:
            # add row with date and total
            print('%s: inserting row=2: %s' % (sheet.title, val))
            sheet.insert_row([mdy, '=sum(c2:au2)', val], index=2, value_input_option='USER_ENTERED')

    # use EntryA for predictions
    # time, actual, predicted
    prediction = values.get('EntryA', {}).get('prediction')
    if not prediction:
        print('no prediction')
        return
    # A     B      C           D              E           F              G           H
    # date, total, 1pm actual, 1pm predicted, 4pm actual, 4pm predicted, 5pm actual, 5pm predicted
    hour_col = {1: ('C', 'D'), 16: ('E', 'F'), 17: ('G', 'H')}
    col = hour_col.get(now_pt.hour, ('C', 'D'))
    sheet = ss.get_worksheet(worksheets['prediction'])
    latest = date_parser.parse(sheet.range('A2:A2')[0].value).date()
    if now_pt.date() == latest:
        # row for this day already exists; update cell
        print('prediction: updating row=2 col=%s: actual=%s predicted=%s' % (
            col, prediction['actual'], prediction['predicted']))
        sheet.update_acell('%s2' % col[0], prediction['actual'])
        sheet.update_acell('%s2' % col[1], prediction['predicted'])
    else:
        # add row with date and total
        print('prediction: inserting row=2: %s' % (val))
        sheet.insert_row([mdy, '=EntryA!B2', prediction['actual'], prediction['predicted']],
                         index=2, value_input_option='USER_ENTERED')


def collect_to_sheet(event, context):
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
    # sheet columns: date total 7:10 AM 7:25 AM 7:40 AM
    # get column for this data point
    if now_pt.hour < 7 or now_pt.hour > 18:
        print(now_pt, ' outside data collection range')
        return
    # predictions M-F at 4pm and 5pm, M-W at 1pm
    alert = {}
    predict = False
    if now_pt.weekday() < 5 and now_pt.minute < 15:
        predict = now_pt.hour in [16, 17]
        # 1pm prediction on Mon-Wed (0-2)
        if now_pt.hour == 13 and now_pt.weekday() in [0, 1, 2]:
            predict = True
    alert_key = 'EntryA'

    # get data from SNAPS
    values = {
        'startTime': start_ts,
        'period': period,
        'time': datetime.now(tz.gettz('America/Los_Angeles')).strftime('%H%M%S'),
    }
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
        predicted = _prediction(now_pt, day[alert_key])
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

    # send to sheet
    print('values=%s' % values)


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

    # predictions M-F at 4pm and 5pm, M-W at 1pm
    alert = {}
    predict = False
    if now_pt.weekday() < 5 and now_pt.minute < 15:
        predict = now_pt.hour in [16, 17]
        # 1pm prediction on Mon-Wed (0-2)
        if now_pt.hour == 13 and now_pt.weekday() in [0, 1, 2]:
            predict = True
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
        predicted = _prediction(now_pt, day[alert_key])
        if not predicted:
            print('no prediction available for %s %s' % (now_pt.hour, day))
            continue
        alert = {
            'key': alert_key,
            'actual': day[alert_key],
            'predicted': predicted
        }
        values[station_type]['prediction'] = {
            'time': now_pt.strftime('%-I%p').lower(),
            'actual': day[alert_key],
            'predicted': predicted
        }
    # send to Keen
    print('values=%s' % values)
    keen.add_event(COLLECTION, values)
    send_alert(alert, now_pt)


if __name__ == '__main__':
    data = {
      "period": 900,
      "exit": {
        "ExitA": 11,
        "ExitB": 11
      },
      "startTime": 1569629146,
      "time": "172546",
      "entry": {
        "EntryB": 11,
        "EntryA": 13
      },
      "EntryA": {
        "prediction": {
            "actual": 220,
            "predicted": 410,
        }
      }
    }
    update_sheet(data, datetime.now())

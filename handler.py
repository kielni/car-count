from datetime import datetime, timedelta
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
from dateutil import tz
from dateutil import parser as date_parser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import urllib3
import xmltodict


"""
    Get data from SNAPS for 15 minute period from 20 minutes ago to 5 minutes ago,
    then send to a Google Sheet.

    Environment parameters
        ALERT_ARN arn:sws:sns:...
        GOOGLE_SHEET_ID
        STATIONS {"entry": "entryStationName", "exit": "exitStationName"}

    Parameters stored in AWS SSM:
        hillbrook-traffic-service-account
        SNAPS_USERNAME
        SNAPS_PASSWORD
"""


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SNAPS_URL = (
    "https://satts11.sensysnetworks.net/snaps/dataservice/stats.xml?"
    + "userName={username}&password={password}&startTime={start_ts}&period={period}&"
    + "locationGroup={station}"
)


def get_param(name: str) -> str:
    """Get a parameter from AWS Parameter store."""
    ssm = boto3.client("ssm")
    param = ssm.get_parameter(Name=name, WithDecryption=True)
    return param["Parameter"]["Value"]


def get_counts(station: str, start_ts: int, period: int) -> Dict[str, int]:
    """Get counts for the specified station and time range.

    start_ts is UTC seconds since the epoch
    period is seconds
    return dictionary of station: count
    SNAPS returns data in XML like this:

    <statistics time="1600453244" period="900">
        <approach name="New Count Station">
            <lanes>
                <lane name="EntryA">
                    <stat volume="2" occupancy="0.14" speedAverage="-1.0"> </stat>
                </lane>
                <lane name="EntryB">
                    <stat volume="2" occupancy="0.29" speedAverage="-1.0"> </stat>
                </lane>
            </lanes>
        </approach>
    </statistics>
    """
    print("request %s for station %s" % (SNAPS_URL, station))
    url = SNAPS_URL.format(
        username=get_param("SNAPS_USERNAME"),
        password=get_param("SNAPS_PASSWORD"),
        start_ts=start_ts,
        period=period,
        station=station,
    )
    # get XML data from SNAPS and parse it
    resp = requests.get(url, verify=False)
    data = xmltodict.parse(resp.text)
    print("data=%s" % data)
    if "statistics" not in data:
        print("error: bad data: %s" % data)
        # notify once an hour during school hours
        now_pt = datetime.now(tz.gettz("America/Los_Angeles"))
        if (
            now_pt.weekday() < 5
            and now_pt.minute < 15
            and now_pt.hour >= 7
            and now_pt.hour <= 17
        ):
            print(
                boto3.client("sns").publish(
                    TopicArn=os.environ["ALERT_ARN"],
                    Message="received bad data from SNAPS:\n\n%s" % resp.text,
                    Subject="error loading traffic data",
                )
            )
        else:
            print("skipping error alert: outside of alert range", now_pt)

        return {}

    values: Dict[str, int] = {}
    print("start: empty values=%s" % values)
    for lane in data["statistics"]["approach"]["lanes"]["lane"]:
        name = lane["@name"]
        values[name] = int(lane["stat"]["@volume"])
        print(
            "lane %s=%s values=%s values[%s]=%s"
            % (name, int(lane["stat"]["@volume"]), values, name, values[name])
        )
    print(
        "station=%s startTime=%s period=%s values=%s"
        % (station, start_ts, period, values)
    )
    return values


def get_spreadsheet() -> gspread.models.Spreadsheet:
    """Use gspread to open the Google sheet using the service account credentials.

    gspread docs: https://gspread.readthedocs.io/en/latest/
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = json.loads(get_param("hillbrook-traffic-service-account"))
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    client = gspread.authorize(credentials)
    return client.open_by_key(os.environ["GOOGLE_SHEET_ID"])


def full_day_from_sheet(sheet_name: str, as_of: datetime):
    ss = get_spreadsheet()
    sheet = ss.worksheet(sheet_name)
    # column B for row matching date
    dt_cell = sheet.find(as_of.strftime("%m/%d/%y"))
    if not dt_cell:
        return -1
    # 4 per hour starting at 5am, plus 2 for date and total
    max_col = (as_of.hour - 5) * 4 + int(as_of.minute / 15) + 2
    values = sheet.row_values(dt_cell.row)[2:max_col]
    return sum([int(v) for v in values])


def _prediction(now_pt: datetime, observed: int) -> int:
    """Get a prediction for the current time and count.

    4pm and 5pm:
        linear regression with Aug 2018-Sep 2019 data
        4pm: coeff=[1.01649594] intercept=44.91931545628796 score=0.6930983959871696
        5pm: coeff=[1.00662224] intercept=17.955353264565133 score=0.8480492171160054
    1pm:
      use threshold for Mon-Wed; don't predict for Thu-Fri
    """
    hour = now_pt.hour
    if hour in [16, 17]:
        # (coef, intercept)
        calc = {
            16: (1.01649594, 44.91931545628796),
            17: (1.00662224, 17.955353264565133),
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


def send_alert(values: Dict[str, int], now_pt: datetime, send=True):
    """Send an alert if high traffic expected.

    values is {'actual': 250, 'predicted': 299}
    pass in now_pt so it can be overridden to test
    """
    # send alert if > 400
    if values.get("predicted", 0) <= 400:
        return
    # early prediction: high or not
    time_str = now_pt.strftime("%-I:%M%p")
    if now_pt.hour == 13:
        subject = "WARNING: high car count predicted as of %s" % (time_str)
        message = "WARNING: %s cars measured as of %s. Over 400 entries predicted." % (
            values["actual"],
            time_str,
        )
    else:
        subject = "WARNING: high car count: %s predicted as of %s" % (
            values["predicted"],
            time_str,
        )
        message = "WARNING: %s cars measured as of %s. %s cars predicted." % (
            values["actual"],
            time_str,
            values["predicted"],
        )
    if send:
        print(
            boto3.client("sns").publish(
                TopicArn=os.environ["ALERT_ARN"], Message=message, Subject=subject
            )
        )
    else:
        print("alert:\n\t%s\n\t%s" % (subject, message))


def update_sheet(values: Dict[str, Dict[str, Any]], now_pt: datetime, write: bool):
    """Update sheet with measured values.

    If write is true, update the spreadsheet; otherwise print updates.
    values looks like {
        'entry': {'EntryA': 0, 'EntryB': 0, 'prediction': {'actual': 250, 'predicted': 299}},
        'exit': {'ExitA': 0, 'ExitB': 0}}
    """
    # setup sheet
    ss = get_spreadsheet()
    # 4 per hour starting at 5am, plus 2 for date and total
    col = (now_pt.hour - 5) * 4 + int(now_pt.minute / 15) + 2
    # sheets: display Exit, display Entry, prediction, EntryA, EntryB, ExitA, ExitB
    worksheets = {"prediction": 2, "EntryA": 3, "EntryB": 4, "ExitA": 5, "ExitB": 6}
    mdy = now_pt.strftime("%-m/%-d/%y")
    prefix = {0: "", 1: "A", 2: "B"}  # A-Z, AA-AZ, BA-BZ
    for key in ["EntryA", "EntryB", "ExitA", "ExitB"]:
        value_key = "entry" if "Entry" in key else "exit"
        if value_key in values:
            val = max(0, values["entry" if "Entry" in key else "exit"].get(key, 0))
        else:
            val = 0
        sheet = ss.get_worksheet(worksheets[key])
        dt_str = sheet.range("A2:A2")[0].value or datetime.now().strftime("%m/%d/%Y")
        latest = date_parser.parse(dt_str).date()
        if now_pt.date() == latest:
            # row for this day already exists; update cell
            cell = "%s%s2" % (prefix[int(col / 26)], chr(65 + col % 26))
            print("%s: updating %s = %s" % (sheet.title, cell, val))
            if write:
                sheet.update_acell(cell, val)
            else:
                print("dry run:\tupdate cell %s = %s" % (cell, val))
        else:
            # add row with date and total
            print("%s: inserting %s %s" % (sheet.title, mdy, val))
            row: List[Any] = [mdy, "=sum(c2:bf2)"] + [""] * 56
            row[col] = val
            if write:
                sheet.insert_row(row, index=2, value_input_option="USER_ENTERED")
            else:
                print("dry run:\tinsert row: %s" % row)

    # prediction': {'actual': 250, 'predicted': 299}
    # without predicted at end of day
    prediction: Dict[str, int] = values.get("entry", {}).get("prediction", {})
    if not prediction:
        print("no prediction")
        return
    # end of day has actual but not predicted; save only if it's high
    if "predicted" not in prediction and prediction["actual"] < 400:
        print(
            "end of day low (%s); not writing to prediction sheet"
            % prediction["actual"]
        )
        return
    # A     B      C           D              E           F              G           H
    # date, total, 1pm actual, 1pm predicted, 4pm actual, 4pm predicted, 5pm actual, 5pm predicted
    hour_col = {1: ("C", "D"), 16: ("E", "F"), 17: ("G", "H"), 18: ("B", None)}
    col_idx: Optional[Tuple[str, Optional[str]]] = hour_col.get(now_pt.hour, None)
    if not col_idx:
        print("invalid hour for prediction sheet: %s" % now_pt.hour)
        return
    sheet = ss.get_worksheet(worksheets["prediction"])
    latest = date_parser.parse(sheet.range("A2:A2")[0].value).date()
    if now_pt.date() != latest:
        # add row with date and total
        print("prediction: inserting row=2: %s" % (val))
        row = [mdy, "=VLOOKUP(A2, EntryA!A:B, 2, FALSE)"]
        if write:
            sheet.insert_row(row, index=2, value_input_option="USER_ENTERED")
        else:
            print("dry run:\tprediction insert row: %s" % row)
    if "predicted" not in prediction:
        return
    # row for this day already exists; update cell
    print(
        "prediction: updating row=2 col=%s: actual=%s predicted=%s"
        % (col_idx, prediction["actual"], prediction["predicted"])
    )
    if write:
        sheet.update_acell("%s2" % col_idx[0], prediction["actual"])
        sheet.update_acell("%s2" % col_idx[1], prediction["predicted"])
    else:
        print("dry run:\tprediction update %s2 = %s" % (col_idx[0], prediction["actual"]))
        print(
            "dry run:\tprediction update %s2 = %s" % (col_idx[1], prediction["predicted"])
        )


def collect_to_sheet(event, context):
    """Collect data from SNAPS and write to Google sheet.

    event and context are provided by AWS Lambda.
    """
    # start 20 minutes ago, in UTC
    start = datetime.now() - timedelta(minutes=20)
    start.replace(second=0, microsecond=0)
    start_ts = int(time.mktime(start.timetuple()))
    # 15 minutes in seconds
    period = 15 * 60
    # start of Pacfic time day
    pt_tz = tz.gettz("America/Los_Angeles")
    # if running for a specific date (for testing)
    if event.get("dt", None):
        now_pt = date_parser.parse(event["dt"]).replace(tzinfo=pt_tz)
        day_start = date_parser.parse(event["dt"]).replace(tzinfo=pt_tz)
        print("running for %s" % now_pt)
    else:
        now_pt = datetime.now(pt_tz)
        day_start = datetime.now(pt_tz)
    day_start = day_start.replace(hour=0, minute=0, second=0)
    day_start_ts = int(time.mktime(day_start.astimezone(tz.gettz("UTC")).timetuple()))
    # seconds between start of day and 5 minutes ago
    day_period = now_pt.hour * 3600 + now_pt.minute * 60 + now_pt.second - 5 * 60
    # sheet columns: date total 5:10 AM 5:25 AM 5:40 AM..
    # get column for this data point
    if now_pt.hour < 5 or now_pt.hour > 18:
        print(now_pt, " outside data collection range")
        return
    # predictions M-F at 4pm and 5pm, M-W at 1pm
    predict = False
    if now_pt.weekday() < 5 and now_pt.minute < 15:
        predict = now_pt.hour in [16, 17]
        # 1pm prediction on Mon-Wed (0-2)
        if now_pt.hour == 13 and now_pt.weekday() in [0, 1, 2]:
            predict = True
    full_day = now_pt.hour > 17
    alert_key = "EntryA"
    # get data from SNAPS
    values = {
        "startTime": start_ts,
        "period": period,
        "time": datetime.now(tz.gettz("America/Los_Angeles")).strftime("%H%M%S"),
    }
    stations = json.loads(os.environ["STATIONS"])
    for station_type in stations:
        print("\nstation %s" % station_type)
        # 15 minutes of data starting 20 minutes ago
        values[station_type] = get_counts(stations[station_type], start_ts, period)
        if not predict and not full_day:
            print(
                "skipping prediction: hour=%s weekday=%s minute=%s"
                % (now_pt.hour, now_pt.weekday(), now_pt.minute)
            )
            continue
        # get full day counts up to 5 minutes ago
        # this seems to be unreliable, often returning -1
        day = get_counts(stations[station_type], day_start_ts, day_period)
        print(
            "hour=%s start=%s (%s) period=%s full day=%s"
            % (
                now_pt.hour,
                day_start_ts,
                datetime.fromtimestamp(day_start_ts),
                day_period,
                day,
            )
        )
        day_count = day.get(alert_key, -1)
        if day_count < 0:
            print("bad data for full day: %s = %s" % (alert_key, day))
            # try to get from sheet
            day_count = full_day_from_sheet(alert_key, now_pt) or -1
            print("full day count from sheet = %s" % day_count)
        if day_count < 150:
            print("actual %s; not enough for a prediction (likely not a school day)" % day_count)
            continue
        values[station_type]["prediction"] = {
            "actual": day_count,
        }
        if not predict:
            continue
        predicted = _prediction(now_pt, day_count)
        print("predicted=%s" % predicted)
        if predicted:
            values[station_type]["prediction"]["predicted"] = predicted
        else:
            print("no prediction available for %s %s" % (now_pt.hour, day))

    # send to sheet
    print("\nvalues=%s" % values)
    update_sheet(values, now_pt, event.get("write", True))
    send_alert(
        values.get("entry", {}).get("prediction", {}), now_pt, event.get("alert", True)
    )


if __name__ == "__main__":
    collect_to_sheet({"write": False, "alert": False, "dt": "2020-09-18 15:10"}, {})

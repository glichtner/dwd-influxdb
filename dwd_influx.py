#!/usr/bin/env python3
import argparse
import csv
import logging
import zipfile
from datetime import datetime
from io import BytesIO, TextIOWrapper

import requests
import yaml
from bs4 import BeautifulSoup
from influxdb_client import InfluxDBClient, Point, WritePrecision

###############################################################################
# Logging Configuration
###############################################################################
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

###############################################################################
# Config / Constants
###############################################################################

BASE_URL = (
    "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate"
)

PERIOD_STRINGS = [
    "1961-1990",
    "1971-2000",
    "1981-2010",
    "1991-2020",
]

MULTI_ANNUAL_MEANS_URL = f"{BASE_URL}/multi_annual"
MULTI_ANNUAL_TEMPLATES = {
    "precipitation": f"{MULTI_ANNUAL_MEANS_URL}/mean_{{short_period}}/Niederschlag_{{period}}.txt",
    "temperature": f"{MULTI_ANNUAL_MEANS_URL}/mean_{{short_period}}/Temperatur_{{period}}.txt",
}

TEN_MINUTE_DATA_URL = {
    "precipitation": f"{BASE_URL}/10_minutes/precipitation",
    "temperature": f"{BASE_URL}/10_minutes/air_temperature",
}

HISTORICAL_URLS = {
    "precipitation": TEN_MINUTE_DATA_URL["precipitation"] + "/historical/",
    "temperature": TEN_MINUTE_DATA_URL["temperature"] + "/historical/",
}

RECENT_URLS = {
    "precipitation": TEN_MINUTE_DATA_URL["precipitation"] + "/recent/",
    "temperature": TEN_MINUTE_DATA_URL["temperature"] + "/recent/",
}

NOW_URLS = {
    "precipitation": TEN_MINUTE_DATA_URL["precipitation"] + "/now/",
    "temperature": TEN_MINUTE_DATA_URL["temperature"] + "/now/",
}


###############################################################################
# Helper Functions
###############################################################################
def load_config(config_path: str) -> dict:
    """
    Reads a YAML configuration file, e.g. station IDs, InfluxDB connection parameters, etc.

    Example config (config.yaml):

    influxdb:
      url: "http://localhost:8086"
      token: "my-secret-token"
      org: "my-org"
      bucket: "dwd"

    stations:
      - "00091"
      - "00103"
      - "13965"

    """
    logger.info("Loading configuration from %s", config_path)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config


def download_file(url: str) -> bytes:
    """
    Downloads the file from the given URL and returns the raw content as bytes.
    """
    logger.info("Downloading file from URL: %s", url)
    r = requests.get(url, timeout=10)
    try:
        r.raise_for_status()
    except Exception as e:
        logger.error("Error downloading %s: %s", url, e)
        raise
    logger.info("Downloaded %s (%d bytes)", url, len(r.content))
    return r.content


def parse_multi_annual_means(content: str):
    """
    Parse multi-annual means text data (either precipitation or temperature).
    Each line looks like:
       Stations_id;Bezugszeitraum;Datenquelle;Jan.;Feb.;März;Apr.;...;Jahr;

    Returns a list of dicts with keys:
    {
      "station_id": ...,
      "reference_period": ...,
      "Jan": ...,
      "Feb": ...,
      ...
    }
    """
    logger.info("Parsing multi-annual means data")
    lines = content.splitlines()
    results = []
    for line in lines:
        line = line.strip()
        # Skip empty lines or the header
        if not line or line.startswith("Stations_id"):
            continue

        # Basic splitting by semicolon, then strip
        parts = [p.strip() for p in line.split(";")]
        # Make sure we handle the correct columns
        # Example indexing:
        # 0: station_id
        # 1: Bezugszeitraum
        # 2: Datenquelle
        # 3..15: Jan..Dez, and 16: Jahr
        try:
            station_id = parts[0]
            ref_period = parts[1]
            jan_val = float(parts[3].replace(",", "."))
            feb_val = float(parts[4].replace(",", "."))
            mar_val = float(parts[5].replace(",", "."))
            apr_val = float(parts[6].replace(",", "."))
            may_val = float(parts[7].replace(",", "."))
            jun_val = float(parts[8].replace(",", "."))
            jul_val = float(parts[9].replace(",", "."))
            aug_val = float(parts[10].replace(",", "."))
            sep_val = float(parts[11].replace(",", "."))
            oct_val = float(parts[12].replace(",", "."))
            nov_val = float(parts[13].replace(",", "."))
            dec_val = float(parts[14].replace(",", "."))
            year_val = float(parts[15].replace(",", ".")) if parts[15] else None

            results.append(
                {
                    "station_id": station_id,
                    "reference_period": ref_period,
                    "Jan": jan_val,
                    "Feb": feb_val,
                    "Mar": mar_val,
                    "Apr": apr_val,
                    "May": may_val,
                    "Jun": jun_val,
                    "Jul": jul_val,
                    "Aug": aug_val,
                    "Sep": sep_val,
                    "Oct": oct_val,
                    "Nov": nov_val,
                    "Dec": dec_val,
                    "Year": year_val,
                }
            )
        except (IndexError, ValueError) as e:
            logger.warning("Skipping malformed line: %s; Error: %s", line, e)
            continue
    logger.info("Parsed %d records from multi-annual means data", len(results))

    return results


def parse_10min_precip(csv_content: str):
    """
    Parse 10-min precipitation data from a CSV chunk inside the zip.
    Example lines:
       STATIONS_ID;MESS_DATUM;QN;RWS_DAU_10;RWS_10;RWS_IND_10;eor

    We only need MESS_DATUM, RWS_10.
    Return list of dicts, e.g.:
    {
      "station_id": ...,
      "timestamp": datetime object,
      "precip_10min": float or None
    }
    """
    logger.info("Parsing 10-minute temperature data")
    out = []
    reader = csv.reader(csv_content.splitlines(), delimiter=";")
    for row in reader:
        if not row or row[0].startswith("STATIONS_ID"):
            continue
        try:
            station_id = row[0].strip()
            mess_datum = row[1].strip()  # e.g. "202001010000"
            rws_10 = row[4].strip()

            # Convert time
            # MESS_DATUM is YYYYMMDDhhmm (12 digits)
            dt = datetime.strptime(mess_datum, "%Y%m%d%H%M")

            # Convert precipitation to float (check for -999, which is missing)
            precip_val = float(rws_10) if rws_10 != "-999" else None

            if precip_val is None:
                continue

            out.append(
                {"station_id": station_id, "time": dt, "precip_10min": precip_val}
            )
        except (IndexError, ValueError) as e:
            logger.warning("Skipping row in temperature data: %s; Error: %s", row, e)
            continue

    logger.info("Parsed %d temperature records", len(out))

    return out


def parse_10min_temp(csv_content: str):
    """
    Parse 10-min temperature/humidity data from CSV chunk inside zip.
    Example lines:
       STATIONS_ID;MESS_DATUM;QN;PP_10;TT_10;TM5_10;RF_10;TD_10;eor

    We want TT_10 (temp) and RF_10 (rel humidity).
    Return list of dicts, e.g.:
    {
      "station_id": ...,
      "time": datetime,
      "temperature_10min": float or None,
      "humidity_10min": float or None
    }
    """
    out = []
    reader = csv.reader(csv_content.splitlines(), delimiter=";")
    for row in reader:
        if not row or row[0].startswith("STATIONS_ID"):
            continue
        try:
            station_id = row[0].strip()
            mess_datum = row[1].strip()  # e.g. "202001010000"
            tt_10 = row[4].strip()
            rf_10 = row[6].strip()

            dt = datetime.strptime(mess_datum, "%Y%m%d%H%M")

            temp_val = float(tt_10) if tt_10 != "-999" else None
            rh_val = float(rf_10) if rf_10 != "-999" else None

            if temp_val is None and rh_val is None:
                continue

            out.append(
                {
                    "station_id": station_id,
                    "time": dt,
                    "temperature_10min": temp_val,
                    "humidity_10min": rh_val,
                }
            )
        except (IndexError, ValueError):
            continue

    return out


def write_points_to_influx(
    client, bucket, org, data_list, measurement, station_map=None
):
    """
    Writes data_list (list of dict) into InfluxDB 2.x using the given measurement name.
    Each dict must contain "station_id" and "time" (if it's a time-based measurement).
    Additional fields are put as fields in Influx.
    """
    logger.info(
        "Writing %d points to InfluxDB for measurement '%s'",
        len(data_list),
        measurement,
    )

    write_api = client.write_api()
    points = []

    for entry in data_list:
        station_id = entry.pop("station_id", None)
        tstamp = entry.pop("time", None)

        p = Point(measurement)
        if station_id:
            p = p.tag("station_id", station_id)
            if station_map and station_id in station_map and station_map[station_id]:
                p = p.tag("station_name", station_map[station_id])
        if tstamp:
            p = p.time(tstamp, WritePrecision.S)

        # The rest of the keys become fields
        for k, v in entry.items():
            p = p.field(k, v)
        points.append(p)

    if points:
        write_api.write(bucket=bucket, org=org, record=points)
        logger.info("Successfully written %d points", len(points))
    else:
        logger.info("No points to write for measurement '%s'", measurement)


###############################################################################
# Main Logic
###############################################################################
def list_dwd_files(base_url: str, prefix: str, suffix: str) -> list:
    """
    Parses the DWD index page at base_url and returns all filenames matching
    the given prefix and suffix.

    Example:
        prefix="10minutenwerte_nieder_"
        suffix="_hist.zip"
    """
    logger.info("Listing files from DWD URL: %s", base_url)
    html = download_file(base_url).decode("utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    files = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and href.startswith(prefix) and href.endswith(suffix):
            files.append(href)

    logger.info(
        "Found %d files matching prefix '%s' and suffix '%s'",
        len(files),
        prefix,
        suffix,
    )

    return files


def fetch_and_write_zip(
    full_url, influx_client, bucket, org, data_type="precipitation", station_map=None
):
    """
    Fetch a zip file from the given URL, extract the text files inside,
    parse them, and write the data to InfluxDB.
    """
    logger.info("Processing zip file: %s", full_url)
    try:
        zip_content = download_file(full_url)
    except Exception as e:
        logger.error("Could not download %s: %s", full_url, e)
        return

    with zipfile.ZipFile(BytesIO(zip_content)) as zf:
        for info in zf.infolist():
            if info.filename.endswith(".txt"):
                logger.info("Processing file inside zip: %s", info.filename)
                with zf.open(info) as f:
                    text = TextIOWrapper(f, encoding="utf-8", errors="ignore").read()
                    if data_type == "precipitation":
                        parsed = parse_10min_precip(text)
                        measurement_name = "precip_10min"
                    else:
                        parsed = parse_10min_temp(text)
                        measurement_name = "temp_10min"

                    write_points_to_influx(
                        influx_client,
                        bucket,
                        org,
                        parsed,
                        measurement_name,
                        station_map,
                    )
                    logger.info(
                        "Wrote %d points for measurement '%s'",
                        len(parsed),
                        measurement_name,
                    )


def fetch_multi_annual_means(
    influx_client,
    bucket,
    org,
    data_type="precipitation",
    station_map=None,
    station_ids=None,
):
    """
    Download multi-annual mean data from the relevant URLs,
    parse, and write them to Influx, *one data point per station-month*.
    Only writes data for stations in `station_ids`.
    """
    # We'll store each month as a separate data point in the measurement:
    #   "multi_annual_precipitation" or "multi_annual_temperature".
    # That way you can easily plot, e.g. monthly precipitation references or do ratios.

    measurement_name = "multi_annual_" + data_type

    for period in PERIOD_STRINGS:
        short_period = "-".join([year[2:] for year in period.split("-")])
        url = MULTI_ANNUAL_TEMPLATES[data_type].format(
            short_period=short_period, period=period
        )

        logger.info("Fetching multi-annual means from URL: %s", url)
        content_bytes = download_file(url)
        content_str = content_bytes.decode("utf-8", errors="ignore")

        # Parse the entire file
        rows = parse_multi_annual_means(content_str)

        # We'll collect data points to write to Influx in one batch
        data_points = []

        for row in rows:
            station_id = row["station_id"]

            # Only proceed if this station is in our config
            if station_ids and station_id not in station_ids:
                continue

            ref_period = row["reference_period"]  # e.g. "1961-1990"

            # We have row["Jan"], row["Feb"], ...
            # Let's map month names to numeric offsets
            month_values = {
                1: row["Jan"],
                2: row["Feb"],
                3: row["Mar"],
                4: row["Apr"],
                5: row["May"],
                6: row["Jun"],
                7: row["Jul"],
                8: row["Aug"],
                9: row["Sep"],
                10: row["Oct"],
                11: row["Nov"],
                12: row["Dec"],
            }

            # We'll pick a reference year from the period (e.g. the start year)
            try:
                start_year = int(ref_period.split("-")[0])  # e.g. 1961
            except ValueError | TypeError:
                logger.warning(
                    "Skipping row with invalid reference period: %s", ref_period
                )
                continue

            # For each month, create a separate point
            for month_num, val in month_values.items():
                # Synthetic time: first day of that month in the start year
                # e.g. 1961-01-01 for January
                # or you might prefer storing all months as day=1, year=1971, etc.
                # The important part is that each month is a separate time value
                synthetic_ts = datetime(start_year, month_num, 1)

                # Create a single data record to transform into an Influx point
                point_dict = {
                    "station_id": station_id,
                    "time": synthetic_ts,
                    "reference_period": ref_period,
                    "value": val,  # The monthly mean precipitation/temperature
                }
                data_points.append(point_dict)

        # Write all data points for that reference period (and data_type)
        write_points_to_influx(
            influx_client, bucket, org, data_points, measurement_name, station_map
        )
        logger.info(
            "Wrote %d monthly means for reference period %s (%s)",
            len(data_points),
            period,
            data_type,
        )


def fetch_historical_10min_data(
    influx_client, bucket, org, station_ids, data_type="precipitation", station_map=None
):
    """
    For each station in station_ids, look up all the historical .zip files from
    the DWD directory (10minutenwerte_...). Then parse and write to Influx.

    In real code, you'd list the directory contents from DWD (HTML parse or something).
    For demonstration, we show how you *would* handle a known URL or partial URL.
    """
    base_url = HISTORICAL_URLS[data_type]

    prefix = f"10minutenwerte_{'nieder' if data_type == 'precipitation' else 'TU'}_"
    suffix = "_hist.zip"
    all_filenames = list_dwd_files(base_url, prefix, suffix)

    # For each station, filter matching files
    for station_id in station_ids:
        padded_id = station_id.zfill(5)
        station_prefix = f"{prefix}{padded_id}_"
        station_files = [fn for fn in all_filenames if fn.startswith(station_prefix)]

        logger.info(
            "Station %s: found %d historical files", station_id, len(station_files)
        )

        for zipfile_name in station_files:
            full_url = base_url + zipfile_name
            fetch_and_write_zip(
                full_url, influx_client, bucket, org, data_type, station_map
            )


def fetch_recent_or_now_10min_data(
    influx_client,
    bucket,
    org,
    station_ids,
    data_type="precipitation",
    period="recent",
    station_map=None,
):
    """
    Similar approach for the "recent" or "now" data from DWD.
    e.g. url = RECENT_URLS[data_type] + f"10minutenwerte_{...}_{station_id}_akt.zip"
    or the "now" path with "_now.zip"
    """
    if period == "recent":
        base_url = RECENT_URLS[data_type]
        suffix = "_akt.zip"
    else:
        base_url = NOW_URLS[data_type]
        suffix = "_now.zip"

    for station_id in station_ids:
        zipfile_name = f"10minutenwerte_{'nieder' if data_type=='precipitation' else 'TU'}_{station_id}{suffix}"
        full_url = base_url + zipfile_name
        fetch_and_write_zip(
            full_url, influx_client, bucket, org, data_type, station_map
        )


def main():
    """
    Main function to handle command line arguments and run the loader.
    """
    parser = argparse.ArgumentParser(description="DWD InfluxDB Loader")
    parser.add_argument(
        "mode",
        choices=["init", "tracking", "historical"],
        help="Run mode: init (one-time - fetching 'recent' data), tracking (repeated - fetching 'now' data ) or historical (fetching historical data)",
    )
    parser.add_argument(
        "--config", default="config.yaml", help="Path to YAML config file."
    )
    args = parser.parse_args()

    logger.info("Starting DWD InfluxDB Loader in '%s' mode", args.mode)

    # Load config
    config = load_config(args.config)

    influx_conf = config.get("influxdb", {})
    influx_url = influx_conf.get("url", "http://localhost:8086")
    influx_token = influx_conf.get("token", "my-token")
    influx_org = influx_conf.get("org", "my-org")
    influx_bucket = influx_conf.get("bucket", "my-bucket")

    stations_config = config.get("stations", [])
    station_map = {}
    station_ids = []
    if isinstance(stations_config, list):
        if all(isinstance(item, dict) for item in stations_config):
            for item in stations_config:
                station_id = item.get("id")
                station_ids.append(station_id)
                station_map[station_id] = item.get("name", "")
        else:
            # Fallback if stations is a list of strings
            station_ids = stations_config
            for station_id in station_ids:
                station_map[station_id] = ""
    else:
        logger.error("Invalid stations configuration format")
        return

    logger.info("Configured station IDs: %s", station_ids)

    # Connect to InfluxDB 2
    with InfluxDBClient(url=influx_url, token=influx_token, org=influx_org) as client:
        if args.mode == "init":
            logger.info("Fetching multi-annual means for precipitation and temperature")
            fetch_multi_annual_means(
                client,
                influx_bucket,
                influx_org,
                "precipitation",
                station_map=station_map,
            )
            fetch_multi_annual_means(
                client,
                influx_bucket,
                influx_org,
                "temperature",
                station_map=station_map,
            )

            logger.info("Fetching recent 10-minute data for precipitation")
            fetch_recent_or_now_10min_data(
                client,
                influx_bucket,
                influx_org,
                station_ids,
                "precipitation",
                period="recent",
                station_map=station_map,
            )
            logger.info("Fetching recent 10-minute data for temperature")
            fetch_recent_or_now_10min_data(
                client,
                influx_bucket,
                influx_org,
                station_ids,
                "temperature",
                period="recent",
                station_map=station_map,
            )
        elif args.mode == "historical":
            logger.info("Fetching historical 10-minute data for precipitation")
            fetch_historical_10min_data(
                client,
                influx_bucket,
                influx_org,
                station_ids,
                "precipitation",
                station_map=station_map,
            )
            logger.info("Fetching historical 10-minute data for temperature")
            fetch_historical_10min_data(
                client,
                influx_bucket,
                influx_org,
                station_ids,
                "temperature",
                station_map=station_map,
            )
        elif args.mode == "tracking":
            logger.info("Fetching now data for precipitation")
            fetch_recent_or_now_10min_data(
                client,
                influx_bucket,
                influx_org,
                station_ids,
                "precipitation",
                period="now",
                station_map=station_map,
            )
            logger.info("Fetching now data for temperature")
            fetch_recent_or_now_10min_data(
                client,
                influx_bucket,
                influx_org,
                station_ids,
                "temperature",
                period="now",
                station_map=station_map,
            )


if __name__ == "__main__":
    main()

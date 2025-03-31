# DWD InfluxDB Loader

This project is a Python-based utility that downloads weather data from the Deutscher Wetterdienst (DWD) and writes it into InfluxDB 2.x. It supports both a one‑time initialization mode (for historical data) and a regular tracking mode (for recent data every 10 minutes).

## Features

- **Configurable Stations:** Define weather stations with both their IDs and human‑readable names.
- **Historical Data Import:** Downloads multi‑annual means and 10‑minute historical weather data (precipitation and temperature).
- **Regular Tracking:** Fetches recent and “now” weather data from DWD and writes new points to InfluxDB.
- **Time-Series Storage:** Uses InfluxDB’s native timestamp and tag support to ensure data consistency.
- **Systemd Service Integration:** Includes an install script with systemd timer support so the tracking mode runs every 10 minutes.
- **Detailed Logging:** Provides informative logging for ease of debugging and monitoring.

## Requirements

- Python 3.6+
- InfluxDB 2.x
- Python dependencies (listed in `requirements.txt`):
  - `requests`
  - `pyyaml`
  - `influxdb-client`
  - `beautifulsoup4`

## Installation

1. **Clone the Repository**

```bash
git clone https://github.com/yourusername/dwd-influxdb-loader.git
cd dwd-influxdb-loader
```

2. **Install Python Dependencies**

Install the required Python packages:

```bash
pip3 install -r requirements.txt
```

3. **Configure the Application**

Create or modify the `config.yaml` file in the project root. For example:

```yaml
influxdb:
  url: "http://localhost:8086"
  token: "my-secret-token"
  org: "my-org"
  bucket: "dwd"

stations:
  - id: "00427"
    name: "Schöneberg"
  - id: "00403"
    name: "Dahlem"
  - id: "00433"
    name: "Tempelhof"
```

4. **Install as a Service**

The project includes an install script (`install.sh`) along with systemd template files in the `systemd/` directory. The install script will:

- Generate the service file with dynamic paths.
- Copy the service and timer files to `/etc/systemd/system/`.
- Install Python dependencies (if needed).
- Enable and start a systemd timer to run the tracking mode every 10 minutes.

To install, run:

```bash
./install.sh
```

## Usage

### Initialization Mode

Run this mode **once** to import historical and multi‑annual data:

```bash
python3 dwd_influx.py init --config config.yaml
```

### Tracking Mode

Run this mode to fetch recent weather data periodically. This mode is intended to be scheduled (e.g., via systemd timer):

```bash
python3 dwd_influx.py tracking --config config.yaml
```

If you used the install script, the systemd timer will run the tracking mode every 10 minutes automatically.

## Logging

The application uses Python’s `logging` module to output detailed logs. You can adjust the log level within the script if needed.

## Contributing

Contributions are welcome! Please open issues or submit pull requests if you have improvements or bug fixes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Weather data is provided by [DWD](https://opendata.dwd.de/).
- The InfluxDB Python client is maintained by InfluxData.

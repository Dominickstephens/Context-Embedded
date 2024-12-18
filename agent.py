import serial
import struct
import yaml
import lib_metrics.metrics_api as metrics_api
import lib_gather_metrics.generateMetric as generateMetric
import threading
import requests
import os
import sys
import time

# Load configuration
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Serial port configuration
SERIAL_PORT = config['agent']['com']  # Replace with your port
BAUD_RATE = 115200

MetricsAPI = metrics_api.MetricsAPI(config['api']['url'])


def check_reboot():
    while True:
        response = requests.get(config['api']['url'] + config['api']['reboot'])
        response_data = response.json()  # Parse the JSON response

        if response_data.get("message") == config['api']['reboot_msg']:
            print("Rebooting...")
            os.execv(sys.executable, ['python'] + sys.argv)
        else:
            # wait for 5 seconds before checking again
            time.sleep(5)


def calculate_checksum(data):
    """Calculate XOR checksum."""
    checksum = 0
    for byte in data:
        checksum ^= byte
    return checksum


def deserialize_data(data):
    """Deserialize received data."""
    if len(data) != 6:
        raise ValueError(f"Incomplete data received. Got {len(data)} bytes, expected 6.")

    # Parse fields (state: uint8, duration_ms: uint32, checksum: uint8)
    state, duration_ms = struct.unpack("<BI", data[:5])
    received_checksum = data[5]

    # Calculate checksum
    calculated_checksum = calculate_checksum(data[:5])

    if received_checksum != calculated_checksum:
        raise ValueError("Checksum mismatch")

    return state, duration_ms


def send_metrics(data):
    metrics = [
        MetricsAPI.DTO_Metric(name="state", value=data[0]),
        MetricsAPI.DTO_Metric(name="duration_ms", value=data[1])
    ]

    data_snapshot = MetricsAPI.DTO_DataInsight(metrics=metrics)

    # Create DTO_Device object
    device = MetricsAPI.DTO_Device(name="device_name", data_snapshots=[data_snapshot])

    # Create DTO_Aggregator object
    aggregator = MetricsAPI.DTO_Aggregator(guid="aggregator_guid", name="aggregator_name", devices=[device])

    # Submit data to API
    MetricsAPI.submit_datasnapshot(aggregator)
    pass


# start a thread that does a get request at https://context-embedded.onrender.com/reboot
# if the message is perform reboot, the program restarts

# Start the thread
reboot_thread = threading.Thread(target=check_reboot)
reboot_thread.daemon = True
reboot_thread.start()

#
# while True:
#     print("Waiting for data...")
#     time.sleep(5)


# Open serial port
with serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1) as ser:
    print("Waiting for data...")
    while True:
        raw_data = ser.read(6)  # Expecting 6 bytes
        if raw_data:
            try:
                state, duration_ms = deserialize_data(raw_data)
                generated = generateMetric.generate_metric()
                send_metrics((state, duration_ms))
                send_metrics(generated)
                print(f"State: {state}, Duration: {duration_ms} ms")
            except ValueError as e:
                print(f"Error: {e}")

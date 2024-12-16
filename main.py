# Import packages
from dash import Dash, html, dash_table, dcc, callback, Output, Input, State
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
from flask import Flask, jsonify, request
from lib_metrics.metrics_datamodel import DTO_Aggregator, DTO_Device, DTO_DataSnapshot, DTO_Metric
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session, sessionmaker
from datetime import datetime, timezone
from models import *
import yaml
import logging

# Load configuration
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup database engine and session
engine = create_engine(config['database']['url'])
Session = sessionmaker(bind=engine)


# Attach logger and engine to the server class
class Server:
    def __init__(self):
        self.logger = logger
        self.engine = engine


server_instance = Server()

# Incorporate data
df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gapminder2007.csv')

# Initialize the app - incorporate a Dash Bootstrap theme
external_stylesheets = [dbc.themes.CERULEAN]
app = Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server  # Access the underlying Flask server

# Initialize a global variable to track reboot status
send_reboot = False

# App layout
app.layout = dbc.Container([
    dbc.Row([
        html.Div('My First App with Data, Graph, and Controls', className="text-primary text-center fs-3")
    ]),

    dbc.Row([
        dbc.RadioItems(options=[{"label": x, "value": x} for x in ['pop', 'lifeExp', 'gdpPercap']],
                       value='lifeExp',
                       inline=True,
                       id='radio-buttons-final')
    ]),

    dbc.Row([
        dbc.Col([
            dash_table.DataTable(data=df.to_dict('records'), page_size=12, style_table={'overflowX': 'auto'})
        ], width=6),

        dbc.Col([
            dcc.Graph(figure={}, id='my-first-graph-final')
        ], width=6),
    ]),

    dbc.Row([
        dbc.Button("Reboot", id="reboot-button", color="danger", className="mt-3")
    ]),

], fluid=True)


# Add controls to build the interaction
@callback(
    Output(component_id='my-first-graph-final', component_property='figure'),
    Input(component_id='radio-buttons-final', component_property='value')
)
def update_graph(col_chosen):
    fig = px.histogram(df, x='continent', y=col_chosen, histfunc='avg')
    return fig


# Callback to handle the reboot button
@callback(
    Output('reboot-button', 'n_clicks'),
    Input('reboot-button', 'n_clicks'),
    prevent_initial_call=True
)
def handle_reboot_button(n_clicks):
    global send_reboot
    send_reboot = True
    return 0


def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if not instance:
        params = {**kwargs, **(defaults or {})}
        instance = model(**params)
        session.add(instance)
        session.flush()
    return instance


# Define an API endpoint
@server.route('/reboot', methods=['GET'])
def reboot():
    global send_reboot
    if send_reboot:
        send_reboot = False
        return jsonify({"message": "Perform reboot"}), 200
    else:
        return jsonify({"message": "No reboot"}), 200


@server.route('/get_metrics', methods=['GET'])
def get_metrics():
    """Get metric values based on optional parameters."""
    try:
        # Using the session factory to create the session within a context manager
        with Session() as session:  # Use the session factory to create the session
            guid = request.args.get('guid')
            device_name = request.args.get('device_name')
            utc_date_min = datetime.strptime(request.args.get('utc_date_min'), '%Y-%m-%d %H:%M:%S') if request.args.get(
                'utc_date_min') else None
            utc_date_max = datetime.strptime(request.args.get('utc_date_max'), '%Y-%m-%d %H:%M:%S') if request.args.get(
                'utc_date_max') else None

            # Start the query from MetricValue and join with MetricSnapshot and Device
            query = session.query(MetricValue). \
                select_from(MetricValue). \
                join(MetricSnapshot, MetricValue.metric_snapshot_id == MetricSnapshot.metric_snapshot_id). \
                join(Device, MetricSnapshot.device_id == Device.device_id)

            # Apply filters based on optional parameters
            if guid:
                query = query.filter(Aggregator.guid == guid)
            if device_name:
                query = query.filter(Device.name == device_name)
            if utc_date_min:
                query = query.filter(MetricSnapshot.client_utc_timestamp_epoch >= int(utc_date_min.timestamp()))
            if utc_date_max:
                query = query.filter(MetricSnapshot.client_utc_timestamp_epoch <= int(utc_date_max.timestamp()))

            metric_values = query.all()

            # Build the DTO hierarchy
            aggregator_dtos = {}
            for metric_value in metric_values:
                device = session.query(Device).filter_by(device_id=metric_value.device_metric_type.device_id).first()
                aggregator = session.query(Aggregator).filter_by(aggregator_id=device.aggregator_id).first()

                aggregator_dto = aggregator_dtos.get(aggregator.guid)
                if not aggregator_dto:
                    aggregator_dto = DTO_Aggregator(
                        guid=aggregator.guid,
                        name=aggregator.name,
                        devices=[]
                    )
                    aggregator_dtos[aggregator.guid] = aggregator_dto

                device_dto = next((d for d in aggregator_dto.devices if d.name == device.name), None)
                if not device_dto:
                    device_dto = DTO_Device(
                        name=device.name,
                        data_snapshots=[]
                    )
                    aggregator_dto.devices.append(device_dto)

                # Find or create a new DataSnapshot for this metric's timestamp
                data_snapshot = next(
                    (ds for ds in device_dto.data_snapshots
                     if
                     ds.timestamp_utc == datetime.fromtimestamp(metric_value.metric_snapshot.client_utc_timestamp_epoch)
                     and ds.timezone_mins == metric_value.metric_snapshot.client_timezone_mins),
                    None
                )

                if not data_snapshot:
                    data_snapshot = DTO_DataSnapshot(
                        timestamp_utc=datetime.fromtimestamp(metric_value.metric_snapshot.client_utc_timestamp_epoch),
                        timezone_mins=metric_value.metric_snapshot.client_timezone_mins,
                        metrics=[]
                    )
                    device_dto.data_snapshots.append(data_snapshot)

                metric_dto = DTO_Metric(
                    name=metric_value.device_metric_type.name,
                    value=metric_value.value,
                )
                data_snapshot.metrics.append(metric_dto)

            return {'status': 'success', 'aggregators': list(aggregator_dtos.values())}, 200

    except Exception as e:
        server_instance.logger.exception("Error in get_metrics route: %s", str(e))
        return {'status': 'error', 'message': str(e)}, 500


@server.route('/get_aggregator', methods=['GET'])
def get_aggregator():
    """Get an existing aggregator by GUID or get all aggregators."""
    guid = request.args.get('guid')
    aggregators = []

    # Using the session factory to create a session within a context manager
    try:
        with Session() as session:  # Use the session factory to create the session
            if guid:
                # Get a single aggregator by GUID
                aggregator = session.query(Aggregator).filter_by(guid=guid).first()
                if not aggregator:
                    return {'status': 'error', 'message': f'No aggregator found with GUID {guid}'}, 404
                aggregators = [aggregator]
            else:
                # Get all aggregators
                aggregators = session.query(Aggregator).all()

    except Exception as e:
        # You can log the error if needed
        server_instance.logger.error(f"Error getting aggregator: {e}")
        return {'status': 'error', 'message': 'An error occurred while fetching the aggregators.'}, 500

    # Convert aggregators to DTO objects
    dto_aggregators = []
    for aggregator in aggregators:
        dto_aggregator = DTO_Aggregator(
            guid=aggregator.guid,
            name=aggregator.name,
            devices=[]
        )
        dto_aggregators.append(dto_aggregator.to_dict())

    return {'status': 'success', 'aggregators': dto_aggregators}, 200


@server.route('/upload_data', methods=['POST'])
def upload_data():
    """Upload aggregator snapshot route."""
    session = None
    try:
        server_instance.logger.info("Deserializing JSON to DTO_Aggregator")
        data = request.get_json()
        dto_aggregator = DTO_Aggregator.from_dict(data)

        server_instance.logger.info(f"JSON deserialized. Storing aggregator snapshot: {dto_aggregator}")
        # server_instance.logger.info(f"Engine: {server_instance.engine}")
        with Session() as session:
            # Get or create aggregator
            aggregator = session.query(Aggregator).filter_by(guid=str(dto_aggregator.guid)).first()
            if not aggregator:
                aggregator = Aggregator(
                    guid=str(dto_aggregator.guid),
                    name=dto_aggregator.name
                )
                session.add(aggregator)
                session.flush()  # Get the ID

            # Process devices and snapshots
            for dto_device in dto_aggregator.devices:
                device = session.query(Device).filter_by(
                    aggregator_id=aggregator.aggregator_id,
                    name=dto_device.name
                ).first()

                if not device:
                    max_ordinal = session.query(Device).filter_by(
                        aggregator_id=aggregator.aggregator_id
                    ).count()
                    device = Device(
                        aggregator_id=aggregator.aggregator_id,
                        name=dto_device.name,
                        ordinal=max_ordinal
                    )
                    session.add(device)
                    session.flush()

                    now_utc = datetime.utcnow()
                    for dto_snapshot in dto_device.data_snapshots:
                        # Validate and convert timestamp_utc
                        if isinstance(dto_snapshot.timestamp_utc, str):
                            dto_snapshot.timestamp_utc = datetime.strptime(dto_snapshot.timestamp_utc,
                                                                           "%Y-%m-%dT%H:%M:%SZ")

                        snapshot = MetricSnapshot(
                            device_id=device.device_id,
                            client_utc_timestamp_epoch=int(dto_snapshot.timestamp_utc.timestamp()),
                            client_timezone_mins=int(dto_snapshot.timezone_mins),
                            server_utc_timestamp_epoch=int(now_utc.timestamp()),
                            server_timezone_mins=int(
                                now_utc.astimezone().utcoffset().total_seconds() / 60) if now_utc.astimezone().utcoffset() else 0
                        )
                        session.add(snapshot)

                    for dto_metric in dto_snapshot.metrics:
                        metric_type = session.query(DeviceMetricType).filter_by(
                            device_id=device.device_id,
                            name=dto_metric.name
                        ).first()

                        if not metric_type:
                            metric_type = DeviceMetricType(
                                device_id=device.device_id,
                                name=dto_metric.name
                            )
                            session.add(metric_type)
                            session.flush()

                        metric_value = MetricValue(
                            metric_snapshot_id=snapshot.metric_snapshot_id,
                            device_metric_type_id=metric_type.device_metric_type_id,
                            value=float(dto_metric.value)
                        )
                        session.add(metric_value)

            session.commit()

        return {'status': 'success', 'message': 'Aggregator snapshot uploaded successfully'}, 201

    except Exception as e:
        if session:
            session.rollback()
        server_instance.logger.error(f"Failed to upload snapshot: {e}")
        return {"status": "error", "message": "Failed to upload snapshot"}, 500


# Run the app
if __name__ == '__main__':
    app.run(debug=True)

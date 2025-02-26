# Context-Embedded

## Overview

The Metric Trends Dashboard is a web application built using Dash and Flask that allows users to visualize metric trends over time from connected ESP32's. The application connects to a database, retrieves metric data, and provides an interactive dashboard for users to analyze trends.

## Features

- Device and Metric Selection: Users can select a device and metric from dropdown menus to visualize trends.

- Interactive Graphs: The application generates line charts using Plotly to display metric values over time.

- Theme Toggle: Users can switch between light and dark themes.

- Reboot Functionality: A reboot button is available to trigger a specific action.

- REST API: The Flask server provides API endpoints for retrieving and uploading metric data.

## Technologies Used

- Dash (for the front-end dashboard UI)

- Flask (for API endpoints and backend logic)

- SQLAlchemy (for database interaction)

- Plotly (for interactive graphs)

- Dash Bootstrap Components (for improved styling)

- Pandas (for data manipulation)

- YAML (for configuration management)

## Configuration

Modify config.yaml to configure the database connection:


## Logging

Logs are managed using Python’s logging module. By default, logs are printed to the console at the INFO level.

## Future Enhancements

- Implement authentication and authorization for API access.

- Improve database query efficiency.

- Add more visualization options and analytics tools.



## Deployed on Render @ https://context-embedded.onrender.com/

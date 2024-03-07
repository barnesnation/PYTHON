import re
import numpy as np
import pandas as pd
import logging
from data_ingestion import read_from_web_CSV

### START FUNCTION 
"""
Module: weather_data_processor.py

This module provides a WeatherDataProcessor class for processing weather data.

Classes:
- WeatherDataProcessor: A class for processing weather data including loading data from a CSV file,
  extracting measurements from messages, and calculating mean values.

Usage:
1. Import the module:
    ```
    import weather_data_processor
    ```

2. Create a WeatherDataProcessor object:
    ```
    config_params = {
        'weather_csv_path': 'https://example.com/weather_data.csv',
        'regex_patterns': {
            'Rainfall': r'(\d+(\.\d+)?)\s?mm',
            'Temperature': r'(\d+(\.\d+)?)\s?C'
        }
    }
    processor = weather_data_processor.WeatherDataProcessor(config_params)
    ```

3. Perform data processing:
    ```
    processor.process()
    ```

4. Access processed data:
    ```
    means = processor.calculate_means()
    ```
"""

class WeatherDataProcessor:
    def __init__(self, config_params, logging_level="INFO"): # Now we're passing in the confi_params dictionary already
        """
    Initialize the WeatherDataProcessor object.

    Args:
    - config_params (dict): A dictionary containing configuration parameters.
                            It should include 'weather_csv_path' for the weather data CSV path,
                            and 'regex_patterns' for regular expression patterns used for data extraction.
    - logging_level (str, optional): The logging level to be used by the logger. Default is 'INFO'.

    Returns:
    - None

    Example:
    ```python
    config_params = {
        'weather_csv_path': 'https://example.com/weather_data.csv',
        'regex_patterns': {
            'Rainfall': r'(\d+(\.\d+)?)\s?mm',
            'Temperature': r'(\d+(\.\d+)?)\s?C'
        }
    }
    processor = WeatherDataProcessor(config_params)
    ```

    Notes:
    - This constructor initializes the WeatherDataProcessor object with the provided configuration parameters.
    - It also initializes the logging system with the specified logging level.
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None  # Initialize weather_df as None or as an empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
    Initialize logging for the WeatherDataProcessor instance.

    Args:
    - logging_level (str): The desired logging level. It can be 'DEBUG', 'INFO', 'NONE', or any other valid logging level.

    Returns:
    - None

    Notes:
    - This method initializes the logger for the WeatherDataProcessor instance with the specified logging level.
    - It configures the logger to prevent log messages from being propagated to the root logger.
    - The logging level can be set to 'DEBUG', 'INFO', or 'NONE' to disable logging.
    - If the logging level is not recognized, it defaults to 'INFO'.
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
    Load weather station mapping data from a CSV file hosted on the web.

    Returns:
    - None

    Notes:
    - This method reads weather station mapping data from a CSV file specified by the `weather_station_data` attribute.
    - It assigns the loaded DataFrame to the `weather_df` attribute of the WeatherDataProcessor instance.
    - It logs a message indicating the successful loading of the weather station data from the web.
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.") 
        # Here, you can apply any initial transformations to self.weather_df if necessary.

    
    def extract_measurement(self, message):
        """
    Extract a measurement from the given message using regular expression patterns.

    Args:
    - message (str): The message from which to extract the measurement.

    Returns:
    - tuple or None: A tuple containing the measurement key and its value if a match is found,
      otherwise returns (None, None).

    Notes:
    - This method iterates over the patterns dictionary provided during object initialization.
    - It searches for a match in the given message using each pattern.
    - If a match is found, it returns a tuple containing the measurement key and its value.
    - If no match is found for any pattern, it returns (None, None).
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
    Process messages in the weather DataFrame to extract measurements.

    Returns:
    - pandas.DataFrame or None: The updated weather DataFrame with extracted measurements
      or None if the weather DataFrame is not initialized.

    Notes:
    - This method applies the extract_measurement method to each message in the weather DataFrame.
    - It adds two new columns 'Measurement' and 'Value' to the DataFrame to store the extracted values.
    - If the weather DataFrame is not initialized, it logs a warning and returns None.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df

    def calculate_means(self):
        """
    Calculate the mean values of measurements grouped by weather station ID and measurement type.

    Returns:
    - pandas.DataFrame or None: A DataFrame containing the mean values of measurements
      grouped by weather station ID and measurement type, or None if the weather DataFrame
      is not initialized.

    Notes:
    - This method computes the mean values of measurements from the weather DataFrame.
    - It groups the data by 'Weather_station_ID' and 'Measurement' columns and calculates
      the mean of the 'Value' column for each group.
    - If the weather DataFrame is not initialized, it logs a warning and returns None.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None
    
    def process(self):
        """
    Perform data processing steps including loading weather station data, extracting measurements from messages,
    and completing the processing pipeline.

    Returns:
    - None

    Notes:
    - This method orchestrates the entire data processing workflow for WeatherDataProcessor.
    - It first loads weather station data and assigns it to the 'weather_df' attribute.
    - Then it processes messages to extract measurements and updates the 'Measurement' and 'Value' columns
      in the 'weather_df'.
    - Finally, it logs a message indicating that the data processing is completed.
        """
        self.weather_station_mapping()  # Load and assign data to weather_df
        self.process_messages()  # Process messages to extract measurements
        self.logger.info("Data processing completed.")

### END FUNCTION
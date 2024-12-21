import os
import logging

def def_logs():
    # Define the logs directory and log file path
    logs_directory = "logs"
    log_file_path = os.path.join(logs_directory, 'meteo_logs.log')

    # Ensure the logs directory exists
    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)

    # Set up logging with an explicit FileHandler
    try:
        # Create a logger and set to DEBUG level to capture all messages
        logger = logging.getLogger("test_logger")
        logger.setLevel(logging.DEBUG)

        # Remove any existing handlers to avoid duplicate logs
        if logger.hasHandlers():
            logger.handlers.clear()

        # Create file handler and set level to info
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.INFO)

        # Create a formatter and set it for the handler
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        logger.addHandler(file_handler)

        # Write a test log message
        logger.info("Test log message - logging setup complete.")

        # Check if log file was created
        if os.path.exists(log_file_path):
            print(f"Log file successfully created at: {log_file_path}")
        else:
            print("Log file was not created. Check configurations and permissions.")
            
            

    
        log_file_path = os.path.join(logs_directory, 'meteo_logs.log')

    
        # Set up the root logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file_path),
                logging.StreamHandler()  # Optionally add console output
            ]
        )

    except Exception as e:
        print(f"Error setting up logging: {e}")

# Run the logging setup
def_logs()




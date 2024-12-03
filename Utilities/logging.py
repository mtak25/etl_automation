import logging
from datetime import datetime
class Logs:
    @staticmethod
    def Log_Gen(log_file_path,loglevel=logging.DEBUG,logger_name='Validations'):
        logs = logging.getLogger(logger_name)
        logs.setLevel(loglevel)

        # Create a timestamp for the log file name
        dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Change format here
        # File handler setup
        log_file_path = f"C:\\Users\\LENOVO\\PycharmProjects\\pythonProject\\ETL_Testing_Automation\\Logs\\Table_validation_Logs_{dt}.log"
        log_file = logging.FileHandler(log_file_path)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%d-%m-%Y %I:%M:%S %p')
        log_file.setFormatter(file_format)

        # Add handlers to the logger
        logs.addHandler(log_file)
        return logs
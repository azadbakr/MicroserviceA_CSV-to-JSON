import pandas as pd
from datetime import datetime
import os
import zmq
import json


context = zmq.Context()
socket = context.socket(zmq.REP)  # REP socket for Microservice
socket.bind("tcp://*:5555")  # Binds the microservice to port 5555


# Function to validate and process the CSV file
def process_csv(csv_file_path):
    # Define the output paths for JSON and text files in the same directory as the CSV file
    json_file_path = os.path.join(os.path.dirname(csv_file_path), "Data.json")
    error_log_path = os.path.join(os.path.dirname(csv_file_path), "ErrorLog.txt")

    # Load the CSV file
    data = pd.read_csv(csv_file_path)

    # Save the entire CSV data as JSON
    data.to_json(json_file_path, orient="records", lines=True)

    # Define a function to check if a value is strictly a number (integer or float)
    def is_number(value):
        if pd.isna(value):  # Check if the value is not a number
            return False
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str) and value.replace('.', '', 1).isdigit():  # String that represents a number
            return True
        return False

    # Define a function to check if a value is a valid date
    def is_date(value, date_format="%m/%d/%Y"):
        try:
            datetime.strptime(value, date_format)
            return True
        except (ValueError, TypeError):
            return False

    # Open the error log file to write the errors
    with open(error_log_path, "w") as file:
        # Iterate through each row and check column values
        for index, row in data.iterrows():

            # Check 'ID' column
            if not is_number(row['ID']):
                file.write(f"Incorrect ID number at row {index + 1}: {row['ID']}\n")

            # Check 'Amount' column
            if not is_number(row['Amount']):
                file.write(f"Incorrect Amount at row {index + 1}: {row['Amount']}\n")

            # Check 'Date' column
            if not is_date(row['Date'], "%m/%d/%Y"):
                file.write(f"Incorrect Date format at row {index + 1}: {row['Date']}\n")

    # Return the paths of the generated files as a response
    return f"JSON File: {json_file_path} \nError Log: {error_log_path}"


# Start the microservice to listen for requests from the main program
print("Microservice is running and waiting for CSV file path ...")
while True:
    message = socket.recv_string()

    # Check for the termination signal
    if message == 'Q':
        socket.send_string("Microservice is shutting down.")
        break

    print(f"Received CSV file path: {message}")

    # Process the CSV file
    try:
        result = process_csv(message)
        response = f"\nStatus: Success! \nFiles: \n{result}"
    except Exception as e:
        response = f"\nStatus: Error, \nMessage: {str(e)}"

    # Send the response back to the main program
    socket.send_string(response)

    print("JSON file and Error Log were created in the same directory")

print("Microservice has been terminated.")
socket.close()
context.destroy()

import pandas as pd
from datetime import datetime
import os
import zmq
import json

context = zmq.Context()
socket = context.socket(zmq.REP)  # REP socket for microservice
socket.bind("tcp://*:5555")  # Binds the microservice to port 5555


# Function to validate and process the CSV file
def process_csv(csv_file_path):
    # Load the CSV file
    data = pd.read_csv(csv_file_path)

    # Convert the entire CSV data to JSON
    json_data = data.to_json(orient="records", lines=False)

    # Define a function to check if a value is strictly a number
    def is_number(value):
        if pd.isna(value):  # Check if the value is NaN
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

    # Create an error log as a dictionary
    errors = []

    # Iterate through each row and check column values
    for index, row in data.iterrows():
        # Check 'ID' column
        if not is_number(row['ID']):
            errors.append({"row": index + 1, "error": f"Incorrect ID: {row['ID']}"})

        # Check 'Amount' column
        if not is_number(row['Amount']):
            errors.append({"row": index + 1, "error": f"Incorrect Amount: {row['Amount']}"})

        # Check 'Date' column
        if not is_date(row['Date'], "%m/%d/%Y"):
            errors.append({"row": index + 1, "error": f"Incorrect Date: {row['Date']}"})

    # Return the JSON data and errors
    return json_data, errors


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
        json_data, errors = process_csv(message)
        response = {
            "status": "success",
            "data": json.loads(json_data),    # Convert JSON string to dictionary
            "errors": errors
        }
    except Exception as e:
        response = {
            "status": "error",
            "message": str(e)
        }

    # Send the response back to the main program
    socket.send_json(response)

print("Microservice has been terminated.")
socket.close()
context.destroy()

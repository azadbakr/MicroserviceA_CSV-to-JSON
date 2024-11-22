import pandas as pd
from datetime import datetime
import zmq

BIND_ADDR = "tcp://*:5555"


# Define a function to check if a value is strictly a number
def is_number(value):
    if pd.isna(value):    # Check if the value is NaN
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str) and value.replace('.', '', 1).isdigit():    # String that represents a number
        return True
    return False


# Define a function to check if a value is a valid date
def is_date(value, date_format="%m/%d/%Y"):
    try:
        datetime.strptime(value, date_format)
        return True
    except (ValueError, TypeError):
        return False


# Function to validate and process the CSV file
def process_csv(csv_file_path):

    data = pd.read_csv(csv_file_path)

    valid_rows = []
    errors = []

    for index, row in data.iterrows():
        row_errors = []

        # Validate 'ID' column
        if not is_number(row['ID']):
            row_errors.append({"row": index + 1, "error": f"Incorrect ID: {row['ID']}"})

        # Validate 'Amount' column
        if not is_number(row['Amount']):
            row_errors.append({"row": index + 1, "error": f"Incorrect Amount: {row['Amount']}"})

        # Validate 'Date' column
        if not is_date(row['Date'], "%m/%d/%Y"):
            row_errors.append({"row": index + 1, "error": f"Incorrect Date: {row['Date']}"})

        if row_errors:
            errors.extend(row_errors)
        else:
            valid_rows.append(row.to_dict())

    return valid_rows, errors


def main():
    # Start the microservice to listen for requests from the main program
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(BIND_ADDR)  
    
    print("Microservice is running and waiting for CSV file path ...")

    while True:
        message = socket.recv_string()

        if message == 'Q':
            socket.send_string("Microservice is shutting down.")
            break

        print(f"Received CSV file path: {message}")

        # Process the CSV file
        try:
            valid_data, errors = process_csv(message)
            response = {
                "status": "success",
                "data": valid_data,    # Only valid rows are included here
                "errors": errors    # Errors are still reported
            }
        except Exception as e:
            response = {
                "status": "error",
                "message": str(e)
            }

        socket.send_json(response)

    print("Microservice has been terminated.")
    socket.close()
    context.destroy()

if __name__ == "__main__":
    main()
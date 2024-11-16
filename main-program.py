import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)  # REQ socket for main program
socket.connect("tcp://localhost:5555")  # Connect to the microservice on port 5555

# Path to the CSV file to send to the microservice
csv_file_path = r'C:\Users\Azadb\PycharmProjects\CS361\Assignment 8\Transactions.csv'

# Send the CSV file path to the microservice
socket.send_string(csv_file_path)

# Receive the response from the microservice
response = socket.recv_json()

# Handle the response
if response["status"] == "success":
    print("JSON Data:")
    print(response["data"])    # JSON data from the microservice
    print("\nErrors:")
    for error in response["errors"]:
        print(f"Row {error['row']}, {error['error']}")
else:
    print("Error:", response["message"])

# Send the termination signal to stop the microservice
socket.send_string("Q")
print("Microservice termination response:", socket.recv_string())

# Close the socket and context
socket.close()
context.term()

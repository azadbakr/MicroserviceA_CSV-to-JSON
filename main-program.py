import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)  # REQ socket for Main Program
socket.connect("tcp://localhost:5555")  # Connect to the microservice on port 5555

# Path to the CSV file to send to the microservice
csv_file_path = r'C:\Users\Azadb\PycharmProjects\CS361\Assignment 8\CSV-to-JSON\Transactions.csv'

# Send the CSV file path to the microservice
socket.send_string(csv_file_path)

# Receive the response from the microservice
response = socket.recv_string()
print("Response from microservice:", response)

# Send the termination signal to stop the microservice
socket.send_string("Q")
response = socket.recv_string()
print("Microservice termination response:", response)

# Close the socket and context
socket.close()
context.term()

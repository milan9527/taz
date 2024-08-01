import pymysql
import time
from datetime import datetime

def connect_to_db():
    while True:
        try:
            conn = pymysql.connect(
                host="proxy-1719849319787-taz1.proxy-c7b8fns5un9o.us-east-1.rds.amazonaws.com",
                user="admin",
                password="xxxxxxxx",
                database="test"
            )
            return conn
        except pymysql.Error as e:
            print(f"Error connecting to MySQL database: {e}")
            time.sleep(1)  # Wait for 1 second before retrying
            continue  # Continue to the next iteration of the loop

while True:
    conn = None
    cursor = None

    try:
        conn = connect_to_db()
        if conn:
            cursor = conn.cursor()
    except pymysql.Error as e:
        print(f"Error connecting to MySQL database: {e}")
        time.sleep(1)  # Wait for 1 second before retrying
        continue  # Continue to the next iteration of the loop

    if cursor:
        while True:
            try:
                # Get the current time with milliseconds
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                # Insert the current time into the database
                sql = "INSERT INTO time_table (ts) VALUES (%s)"
                cursor.execute(sql, (current_time,))
                conn.commit()

                # Select and print the latest row with milliseconds
                cursor.execute("SELECT id, DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s.%f') AS formatted_time FROM time_table ORDER BY id DESC LIMIT 1")
                result = cursor.fetchone()
                if result:
                    print(f"ID: {result[0]}, Time: {result[1]}")
            except pymysql.Error as e:
                error_code = e.args[0]
                if error_code in (1053, 2013, 0):  # Server shutdown, lost connection, or empty error
                    print(f"Error executing SQL statements: {e}")
                    print("Reconnecting to the database...")
                    conn.close()  # Close the current connection
                    conn = connect_to_db()  # Reconnect to the database
                    if conn:
                        cursor = conn.cursor()
                    else:
                        print("Failed to reconnect to the database.")
                        break
                else:
                    print(f"Error executing SQL statements: {e}")
                    time.sleep(1)  # Wait for 1 second before retrying
                    continue  # Continue to the next iteration of the loop

            # Wait for 100 milliseconds (0.1 seconds)
            time.sleep(0.1)

    # Close the database connection
    if conn:
        conn.close()




测试程序rds-insert-proxy.py

import pymysql
import threading
import random
import string
from time import sleep, time

# MySQL connection configuration
config = {
    'host': "rds-proxy.proxy-c7b8fns5un9o.us-east-1.rds.amazonaws.com",
    'user': "admin",
    'password': "wAr16dk7",
    'database': "test"
}

# Function to generate dummy data
def generate_dummy_data(batch_size):
    data_batch = []
    for _ in range(batch_size):
        column1_value = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        column2_value = random.randint(1, 100)
        column3_value = ''.join(random.choices(string.ascii_letters, k=20))
        data_batch.append((column1_value, column2_value, column3_value))
    return data_batch

# Global variables for QPS calculation
total_queries = 0
last_time = time()
lock = threading.Lock()

# Function to insert data into the database
def insert_data():
    batch_size = 10000  # Adjust batch size as needed

    while True:
        try:
            # Connect to the MySQL database
            conn = pymysql.connect(**config)
            cursor = conn.cursor()

            # Generate a batch of dummy data
            data_batch = generate_dummy_data(batch_size)

            while True:
                try:
                    # Insert data into the database in batches
                    query = "INSERT INTO dummy_table (column1, column2, column3) VALUES (%s, %s, %s)"
                    cursor.executemany(query, data_batch)
                    conn.commit()
                    break  # Exit the inner loop if the SQL statement is executed successfully
                except pymysql.Error as err:
                    print("Error inserting data:", err)
                    print("Retrying in 1 second")
                    sleep(1)  # Retry after 1 second

            # Update total_queries with thread-safe locking
            with lock:
                global total_queries
                total_queries += batch_size

            # Close the connection
            conn.close()

            # Introduce a delay to control the insertion rate
            sleep(0.1)  # Adjust the delay as needed

        except pymysql.Error as err:
            print("Error connecting to the database:", err)
            print("Retrying in 1 second")
            sleep(1)  # Retry after 1 second

# Function to calculate and print QPS
def calculate_qps():
    global total_queries, last_time

    while True:
        current_time = time()
        if current_time - last_time >= 10:
            with lock:
                qps = total_queries / (current_time - last_time)
                print(f"Total QPS: {qps:.2f}")
                total_queries = 0
                last_time = current_time
        sleep(1)

# Create and start the worker threads
num_threads = 4  # Number of threads to use
threads = []
for _ in range(num_threads):
    thread = threading.Thread(target=insert_data)
    thread.start()
    threads.append(thread)

# Create and start the QPS calculation thread
qps_thread = threading.Thread(target=calculate_qps)
qps_thread.start()

# Wait for the threads to finish (they will run indefinitely)
for thread in threads:
    thread.join()
qps_thread.join()

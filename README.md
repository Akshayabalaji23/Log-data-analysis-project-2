import re
from datetime import datetime
import pymongo
import mysql.connector
# Step 1: Extract Emails
def extract_emails(file_path):
    """
    Reads the file and extracts email addresses.

    Args:
        file_path (str): Path to the file.

    Returns:
        list: A list of email addresses.
    """
    email_list = []

    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Identify lines starting with "From " indicating email presence
                if line.startswith("From "):
                    parts = line.split()
                    if len(parts) > 1:
                        email = parts[1]  # Second element is usually the email
                        email_list.append(email)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return email_list

# Step 2: Fetch and Format Dates
def fetch_and_format_dates(file_path):
    """
    Reads the file, extracts dates, and formats them as YYYY-MM-DD HH:MM:SS.

    Args:
        file_path (str): Path to the file.

    Returns:
        list: A list of formatted date strings.
    """
    formatted_dates = []

    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Look for lines starting with "Date:"
                if line.startswith("Date:"):
                    raw_date = line.replace("Date:", "").strip()  # Extract the date part
                    try:
                        # Parse the date and format it
                        date_obj = datetime.strptime(raw_date, "%a, %d %b %Y %H:%M:%S %z")
                        formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                        formatted_dates.append(formatted_date)
                    except ValueError:
                        pass  # Skip invalid date formats
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return formatted_dates

# Step 3: Combine Emails and Dates
def combine_emails_and_dates(emails, dates):
    """
    Combines extracted emails and formatted dates into a list of dictionaries.

    Args:
        emails (list): List of email addresses.
        dates (list): List of formatted date strings.

    Returns:
        list: A list of dictionaries with 'email' and 'date' keys.
    """
    return [{"email": email, "date": date} for email, date in zip(emails, dates)]

# Step 4: Save to MongoDB
def save_to_mongodb(email_date_list):
    client = pymongo.MongoClient("mongodb+srv://AkshayaBalaji:Akshayavishal23!@cluster0.bhbve.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client["project"]
    collection = db["user_history"]
    collection.insert_many(email_date_list)
    print("Data saved to MongoDB.")
    client.close()

# Step 5: Save to SQL Database
def save_to_sql_db():
    mydb = mysql.connector.connect(
        host="gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
        port=4000,
        user="ecuxr5DiA7ZosEG.root",
        password="IMSvUztZcZ8axDYq",
        database="project",
    )

    if mydb.is_connected():
        print("Connection successful")
        cursor = mydb.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_history (
                email VARCHAR(255),
                date DATETIME,
                PRIMARY KEY (email, date)
            )
        """)

        # Fetch data from MongoDB
        client = pymongo.MongoClient("mongodb+srv://AkshayaBalaji:Akshayavishal23!@cluster0.bhbve.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        db = client["project"]
        collection = db["user_history"]
        data = list(collection.find({}, {"_id": 0}))

        # Insert data into MySQL
        for record in data:
            cursor.execute("""
                INSERT IGNORE INTO user_history (email, date) VALUES (%s, %s)
            """, (record["email"], record["date"]))

        mydb.commit()
        print("Data saved to SQL database.")
        mydb.close()
        client.close()
    else:
        print("Connection failed")

# Step 6: Run Queries
def run_queries():
    mydb = mysql.connector.connect(
        host="gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
        port=4000,
        user="ecuxr5DiA7ZosEG.root",
        password="IMSvUztZcZ8axDYq",
        database="project",
    )

    if mydb.is_connected():
        cursor = mydb.cursor()

        # Example query1
        cursor.execute("SELECT DISTINCT email FROM user_history")
        print("Unique Email Addresses:", cursor.fetchall())

        cursor.execute("""
            SELECT DATE(date) AS day, COUNT(*) 
            FROM user_history 
            GROUP BY day
        """)
        print("Emails Per Day:", cursor.fetchall())

        # Query 2: Count the number of emails received per day
        cursor.execute("""
            SELECT DATE(date) AS day, COUNT(*) 
            FROM user_history 
            GROUP BY day;
        """)
        print("Emails Per Day:", cursor.fetchall())

        # Query 3: Find the first and last email date for each email address
        cursor.execute("""
            SELECT email, MIN(date) AS first_date, MAX(date) AS last_date
            FROM user_history
            GROUP BY email;
        """)
        print("First and Last Email Date per Email:", cursor.fetchall())

        # Query 4: Count the total number of emails from each domain
        cursor.execute("""
            SELECT SUBSTRING_INDEX(email, '@', -1) AS domain, COUNT(*)
            FROM user_history
            GROUP BY domain;
        """)
        print("Emails Per Domain:", cursor.fetchall())

        # Query 5: Count the Total Number of Emails from Each Domain
        cursor.execute("""   
            SELECT SUBSTRING_INDEX(email, '@', -1) AS domain, COUNT(*) AS email_count
            FROM user_history
            GROUP BY domain
            ORDER BY email_count DESC;
        """)
        print("Total Emails from Each Domain:", cursor.fetchall())

        # Query 6: Find the Top 5 Email Addresses That Sent the Most Emails
        cursor.execute("""   
            SELECT email, COUNT(*) AS email_count
            FROM user_history
            GROUP BY email
            ORDER BY email_count DESC
            LIMIT 5;
        """)
        print("Top 5 Email Addresses that sent the most Emails:", cursor.fetchall())

        # Query 7: Find Emails Sent on a Specific Date (e.g., 2025-01-01)
        cursor.execute("""   
            SELECT email, date
            FROM user_history
            WHERE DATE(date) = '2025-01-01';
        """)
        print("Emails Sent on a Specific Date:", cursor.fetchall())

        # Query 8: Get the Total Number of Unique Domains
        cursor.execute("""   
            SELECT COUNT(DISTINCT SUBSTRING_INDEX(email, '@', -1)) AS unique_domains
            FROM user_history;
        """)
        print("Total Number of Unique Domains:", cursor.fetchall())

        # Query 9: Get the Earliest and Latest Email Dates in the Table
        cursor.execute("""   
            SELECT MIN(date) AS earliest_email, MAX(date) AS latest_email
            FROM user_history;
        """)
        print("Earliest and Latest Email Dates in the Table:", cursor.fetchall())

        # Query 10: Find the Number of Emails Sent Per Hour
        cursor.execute("""   
            SELECT HOUR(date) AS hour, COUNT(*) AS email_count
            FROM user_history
            GROUP BY hour
            ORDER BY hour ASC;
        """)
        print("Number of Emails Sent Per Hour:", cursor.fetchall())

        mydb.close()
    else:
        print("Connection failed")

# Main Execution
if __name__ == "__main__":
    file_path = "D:\\Data Engineering Log Data Analysis Project\\mbox.txt"  # Replace with your file path

    # Extract emails and dates
    emails = extract_emails(file_path)
    dates = fetch_and_format_dates(file_path)

    # Combine emails and dates
    if emails and dates:
        email_date_pairs = combine_emails_and_dates(emails, dates)
        save_to_mongodb(email_date_pairs)
        save_to_sql_db()
        run_queries()
    else:
        print("No valid email-date pairs extracted.")

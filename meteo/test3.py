from datetime import datetime

# Original date string in YYYY-MM-DD format
date_str = "2024-10-24"

# Convert the string to a datetime object
date_obj = datetime.strptime(date_str, "%Y-%m-%d")

# Format the date to DD-MM-YYYY
formatted_date = date_obj.strftime("%d-%m-%Y")

print(formatted_date)

import csv
import random
import os
import math
import time
from multiprocessing import Process, cpu_count
from faker import Faker

# --- Configuration ---
OUTPUT_FILE_NAME = 'large_pii_dataset.csv'
APPROX_BYTES_PER_ROW = 500
TEMP_DIR = 'temp_files'
# --------------------

# Ensure the temporary directory exists
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

def get_user_choice():
    """Gets user input for how to determine the file size."""
    print("How do you want to define the amount of fake data?")
    print("  1. By a specific number of rows.")
    print("  2. By an approximate file size (in MB).")
    while True:
        choice = input("Enter your choice (1 or 2): ")
        if choice in ['1', '2']:
            return choice
        else:
            print("❌ Invalid input. Please enter 1 or 2.")

def get_number_of_rows(choice):
    """Calculates the number of rows based on the user's choice."""
    if choice == '1':
        while True:
            try:
                rows = int(input("Enter the desired number of rows: "))
                if rows > 0: return rows
                else: print("❌ Please enter a positive number.")
            except ValueError:
                print("❌ Invalid input. Please enter a whole number.")
    
    elif choice == '2':
        while True:
            try:
                size_mb = int(input("Enter the desired file size in MB: "))
                if size_mb > 0:
                    target_bytes = size_mb * 1024 * 1024
                    estimated_rows = int(target_bytes / APPROX_BYTES_PER_ROW)
                    print(f"ℹ️  To create a file of approximately {size_mb} MB, I will generate {estimated_rows:,} rows.")
                    return estimated_rows
                else:
                    print("❌ Please enter a positive number for the size.")
            except ValueError:
                print("❌ Invalid input. Please enter a whole number.")

def create_pii_record(faker_instance):
    """Generates a single dictionary representing a row of PII data."""
    full_name = faker_instance.name()
    return {
        "Name": full_name, "Email": faker_instance.email(), "Date of birth": faker_instance.date_of_birth(minimum_age=18, maximum_age=70).strftime('%Y-%m-%d'),
        "Phone Number": faker_instance.phone_number(), "Location": faker_instance.address().replace('\n', ', '), "Postal Code": faker_instance.zipcode(),
        "Gender": random.choice(['Male', 'Female', 'Non-binary']), "Marital Status": random.choice(['Single', 'Married', 'Divorced']),
        "Blood Type": random.choice(['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']), "Religion": random.choice(['Christianity', 'Islam', 'Hinduism', 'Atheist']),
        "Credit Card Number": faker_instance.credit_card_number(), "Card Expiry Date": faker_instance.credit_card_expire(), "CVV/CVC code": faker_instance.credit_card_security_code(),
        "Bank Account Number": faker_instance.bban(), "Swift Code": faker_instance.swift(length=11), "Credit Score": random.randint(300, 850),
        "National ID number": faker_instance.ssn(), "Driver License Number": faker_instance.license_plate(), "Passport Number": faker_instance.passport_number(),
        "Voter ID Number": f"VTR{random.randint(1000000, 9999999)}", "Health Insurance ID": f"H{random.randint(100000000, 999999999)}",
        "IP Address": faker_instance.ipv4(), "Device ID": faker_instance.uuid4(), "Social Media profile links": f"https://linkedin.com/in/{full_name.lower().replace(' ', '-')}",
        "Organization": faker_instance.company(), "Occupation": faker_instance.job(), "Employee ID": f"EMP-{random.randint(1000, 99999)}"
    }

def generate_chunk(start_row, num_rows, temp_filename):
    """A worker function that generates a chunk of data and saves it to a temporary CSV."""
    temp_filepath = os.path.join(TEMP_DIR, temp_filename)
    print(f"  ▶️  Process {os.getpid()} starting to generate {num_rows:,} rows into {temp_filepath}")
    fake = Faker()

    headers = create_pii_record(fake).keys()

    with open(temp_filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for _ in range(num_rows):
            writer.writerow(create_pii_record(fake))
    print(f"  ✅ Process {os.getpid()} finished.")

def combine_files(num_processes, final_filename, headers):
    """Combines all temporary CSV files into one final file."""
    print(f"\n🤝 Combining {num_processes} temporary files into '{final_filename}'...")
    with open(final_filename, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=headers)
        writer.writeheader()

        for i in range(num_processes):
            temp_filename = f'temp_part_{i}.csv'
            temp_filepath = os.path.join(TEMP_DIR, temp_filename)
            try:
                with open(temp_filepath, 'r', encoding='utf-8') as infile:
                    reader = csv.DictReader(infile)
                    for row in reader:
                        writer.writerow(row)
                os.remove(temp_filepath)
            except FileNotFoundError:
                print(f"  ⚠️  Warning: Temporary file {temp_filepath} not found. It might have had no rows to generate.")

    print("🧹 Cleaned up temporary files.")

# --- Main script execution ---
if __name__ == "__main__":
    start_time = time.time()
    
    user_choice = get_user_choice()
    TOTAL_ROWS = get_number_of_rows(user_choice)

    # Use one less than the total number of cores, but always use at least 1 to make sure system does not lag.
    total_cores = cpu_count()
    num_processes = max(1, total_cores - 1) 
    print(f"\n⚙️  Utilizing {num_processes} of {total_cores} available CPU cores to keep the system responsive.")
    
    chunk_size = math.ceil(TOTAL_ROWS / num_processes)
    
    processes = []
    
    for i in range(num_processes):
        start_row = i * chunk_size
        rows_for_this_process = min(chunk_size, TOTAL_ROWS - start_row)
        if rows_for_this_process <= 0: continue
            
        temp_filename = f'temp_part_{i}.csv'
        process = Process(target=generate_chunk, args=(start_row, rows_for_this_process, temp_filename))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    dummy_faker = Faker()
    file_headers = create_pii_record(dummy_faker).keys()
    combine_files(num_processes, OUTPUT_FILE_NAME, file_headers)
    
    end_time = time.time()
    print(f"\n🎉 Success! Your file '{OUTPUT_FILE_NAME}' with {TOTAL_ROWS:,} rows has been created.")
    print(f"⏱️  Total time taken: {end_time - start_time:.2f} seconds.")
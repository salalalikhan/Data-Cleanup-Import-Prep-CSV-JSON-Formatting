#!/usr/bin/env python3

import csv
import random
from datetime import datetime, timedelta

def generate_large_dataset():
    """Generate 500 records with 200 realistic data problems"""
    
    # Base clean data templates
    clean_names = [
        "John Smith", "Maria Garcia", "David Chen", "Sarah Johnson", "Ahmed Hassan",
        "Elena Petrov", "James Wilson", "Priya Sharma", "Carlos Rodriguez", "Lisa Anderson",
        "Mohammad Ali", "Anna Kowalski", "Kevin O'Brien", "Fatima Al-Zahra", "Robert Brown",
        "Sofia Andersson", "Michael Taylor", "Yuki Tanaka", "Emma Thompson", "Diego Martinez"
    ]
    
    problematic_records = []
    clean_records = []
    
    # Generate 200 problematic records
    for i in range(1, 201):
        record = generate_problematic_record(i, clean_names)
        problematic_records.append(record)
    
    # Generate 300 clean records
    for i in range(201, 501):
        record = generate_clean_record(i, clean_names)
        clean_records.append(record)
    
    # Shuffle them together for realistic distribution
    all_records = problematic_records + clean_records
    random.shuffle(all_records)
    
    return all_records

def generate_problematic_record(id_num, names):
    """Generate a record with specific data problems"""
    
    problems = [
        # ID problems
        lambda: {"rec_id": f"ID{id_num}", "issue": "non-numeric prefix"},
        lambda: {"rec_id": f"{id_num}A", "issue": "non-numeric suffix"},
        lambda: {"rec_id": "", "issue": "missing ID"},
        lambda: {"rec_id": f"USER-{id_num}-OLD", "issue": "complex ID format"},
        
        # Name problems
        lambda: {"clientFullName": f"  {random.choice(names)}  ", "issue": "whitespace padding"},
        lambda: {"clientFullName": f"{random.choice(names)},", "issue": "trailing comma"},
        lambda: {"clientFullName": f"{random.choice(names)},,", "issue": "multiple commas"},
        lambda: {"clientFullName": f"Mr.   {random.choice(names)}   Jr.", "issue": "extra spaces"},
        lambda: {"clientFullName": "", "issue": "empty name"},
        lambda: {"clientFullName": "   ", "issue": "whitespace only"},
        
        # Unit problems
        lambda: {"unit": f"Unit #{random.randint(1,50)}", "issue": "unit with symbols"},
        lambda: {"unit": f"Building-{random.randint(1,50)}-Apt-{random.randint(1,20)}", "issue": "complex unit"},
        lambda: {"unit": "", "issue": "empty unit"},
        lambda: {"unit": "???", "issue": "invalid unit"},
        
        # Date problems
        lambda: {"moveIn": f"2024-{random.randint(13,15)}-01", "issue": "invalid month"},
        lambda: {"moveIn": f"2024-02-{random.randint(30,32)}", "issue": "invalid day"},
        lambda: {"moveIn": f"{random.randint(1,12)}/{random.randint(1,28)}/24", "issue": "short year"},
        lambda: {"moveIn": f"{random.randint(1,28)}-{random.randint(1,12)}-2024", "issue": "day-month-year"},
        lambda: {"moveIn": "not a date", "issue": "text date"},
        lambda: {"moveIn": "", "issue": "missing date"},
        lambda: {"moveIn": "2024/13/01", "issue": "invalid date format"},
        
        # Boolean problems
        lambda: {"isActive": "TRUE", "issue": "caps boolean"},
        lambda: {"isActive": "FALSE", "issue": "caps boolean"},
        lambda: {"isActive": "yes", "issue": "text boolean"},
        lambda: {"isActive": "no", "issue": "text boolean"},
        lambda: {"isActive": "1", "issue": "numeric boolean"},
        lambda: {"isActive": "0", "issue": "numeric boolean"},
        lambda: {"isActive": "?", "issue": "invalid boolean"},
        lambda: {"isActive": "", "issue": "missing boolean"},
        
        # Balance problems
        lambda: {"balance": f"${random.randint(100,5000):,}.{random.randint(10,99)}", "issue": "currency with commas"},
        lambda: {"balance": f"({random.randint(100,1000)}.00)", "issue": "negative in parentheses"},
        lambda: {"balance": f"{random.randint(1,10)}k", "issue": "k notation"},
        lambda: {"balance": f"{random.randint(100,1000)}.{random.randint(100,999)}", "issue": "3 decimal places"},
        lambda: {"balance": "", "issue": "missing balance"},
        lambda: {"balance": "N/A", "issue": "text balance"},
        lambda: {"balance": f"€{random.randint(100,2000)}.{random.randint(10,99)}", "issue": "euro currency"},
        lambda: {"balance": f"£{random.randint(100,2000)}.{random.randint(10,99)}", "issue": "pound currency"},
        
        # Email problems
        lambda: {"email": f"user{id_num}@", "issue": "incomplete email"},
        lambda: {"email": f"@domain.com", "issue": "missing username"},
        lambda: {"email": f"user{id_num}domain.com", "issue": "missing @ symbol"},
        lambda: {"email": f"USER{id_num}@DOMAIN.COM", "issue": "caps email"},
        lambda: {"email": "", "issue": "missing email"},
        lambda: {"email": "invalid.email", "issue": "no domain"},
        
        # Phone problems
        lambda: {"phone": f"({random.randint(100,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}", "issue": "formatted phone"},
        lambda: {"phone": f"+1-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}", "issue": "international format"},
        lambda: {"phone": f"{random.randint(100,999)}.{random.randint(100,999)}.{random.randint(1000,9999)}", "issue": "dot separated"},
        lambda: {"phone": f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(1000,9999)}", "issue": "space separated"},
        lambda: {"phone": "", "issue": "missing phone"},
        lambda: {"phone": "123", "issue": "too short"},
        lambda: {"phone": "not-a-phone", "issue": "text phone"},
        
        # Company ID problems
        lambda: {"company_id": f"{random.randint(1,100)}", "issue": "numeric only"},
        lambda: {"company_id": f"COMP{random.randint(1,100)}", "issue": "no padding"},
        lambda: {"company_id": "", "issue": "missing company_id"},
        lambda: {"company_id": "???", "issue": "invalid company_id"},
    ]
    
    # Pick a random problem type
    problem_func = random.choice(problems)
    problem_data = problem_func()
    
    # Create base record
    record = {
        "rec_id": str(1000 + id_num),
        "clientFullName": random.choice(names),
        "unit": f"{random.randint(1,50)}A",
        "moveIn": "2024-03-15",
        "isActive": "Y",
        "balance": f"${random.randint(500,3000)}.00",
        "notes": f"Record {id_num} - {problem_data.get('issue', 'test')}",
        "email": f"user{id_num}@example.com",
        "phone": f"555{random.randint(1000000,9999999)}",
        "company_id": f"COMP{id_num:03d}"
    }
    
    # Apply the problem
    record.update({k: v for k, v in problem_data.items() if k != 'issue'})
    
    return record

def generate_clean_record(id_num, names):
    
    # Various clean formats to show diversity
    date_formats = [
        lambda: f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        lambda: f"{random.randint(1,12)}/{random.randint(1,28)}/2024",
        lambda: f"{random.randint(1,28)}/{random.randint(1,12)}/2024"
    ]
    
    bool_formats = ["Y", "N", "Yes", "No", "True", "False", "1", "0", "Active", "Inactive"]
    
    return {
        "rec_id": str(1000 + id_num),
        "clientFullName": random.choice(names),
        "unit": f"{random.randint(1,50)}{random.choice(['A', 'B', 'C', ''])}",
        "moveIn": random.choice(date_formats)(),
        "isActive": random.choice(bool_formats),
        "balance": f"${random.randint(500,5000)}.{random.randint(0,99):02d}",
        "notes": f"Clean record {id_num}",
        "email": f"user{id_num}@company{random.randint(1,50)}.com",
        "phone": f"555{random.randint(1000000,9999999)}",
        "company_id": f"COMP{id_num:03d}"
    }

def main():
    print("Generating Enterprise-Scale Dataset...")
    print("Target: 500 records with 200 data quality challenges")
    
    records = generate_large_dataset()
    
    # Write to CSV
    output_file = "data/input/legacy_export.csv"
    fieldnames = ["rec_id", "clientFullName", "unit", "moveIn", "isActive", "balance", "notes", "email", "phone", "company_id"]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"Generated {len(records)} records")
    print(f"Saved to: {output_file}")
    print("Ready for enterprise-scale data cleanup demonstration!")

if __name__ == "__main__":
    main()
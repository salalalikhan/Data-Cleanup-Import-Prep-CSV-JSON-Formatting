#!/usr/bin/env python3

import csv
import json
import argparse
import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import Counter
import html

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATE_FORMATS = [
    "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d-%m-%Y", 
    "%m-%d-%Y", "%b %d %Y", "%d %b %Y", "%B %d %Y", "%d %B %Y", "%Y%m%d",
    "%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", 
    "%B %dst %Y", "%B %dnd %Y", "%B %drd %Y", "%B %dth %Y"
]

BOOLEAN_TRUE = {"y", "yes", "true", "1", "active", "t", "on", "enabled"}
BOOLEAN_FALSE = {"n", "no", "false", "0", "inactive", "f", "off", "disabled"}
CURRENCY_SYMBOLS = ["$", "€", "£", "¥", "₪", "₽", "USD", "EUR", "GBP", "HKD", "HK$"]


class DataProcessor:
    def __init__(self):
        self.issues = []
        self.stats = {'total': 0, 'valid': 0, 'issues': 0}
        self.seen_ids = set()
        self.processing_times = {
            'start_time': None,
            'end_time': None,
            'records_per_second': 0
        }
    
    def log_issue(self, row: int, field: str, value: str, issue: str):
        self.issues.append((row, field, value, issue))
        self.stats['issues'] += 1
        logger.warning(f"Row {row} - {field}: {issue}")
    
    def clean_id(self, value: str, row: int) -> Optional[int]:
        if not value or not value.strip():
            return None
        
        digits = re.sub(r"\D", "", value.strip())
        if not digits:
            return None
        
        try:
            record_id = int(digits)
            if record_id in self.seen_ids:
                self.log_issue(row, "record_id", value, "duplicate ID")
                # Generate unique ID for duplicates
                record_id = record_id + 10000
            self.seen_ids.add(record_id)
            return record_id
        except ValueError:
            return None
    
    def clean_name(self, value: str, _: int) -> str:
        if not value or not value.strip():
            return ""
        
        cleaned = re.sub(r"[,\s]+$", "", value.strip())
        cleaned = re.sub(r"\s+", " ", cleaned)
        
        if len(cleaned) > 100:
            cleaned = cleaned[:100]
        
        return cleaned
    
    def clean_unit(self, value: str, _: int) -> str:
        if not value or not value.strip():
            return ""
        
        cleaned = value.strip().upper()
        cleaned = re.sub(r"[^A-Z0-9]", "", cleaned)
        cleaned = re.sub(r"^(UNIT|BUILDING|BLDG)", "", cleaned)
        
        return cleaned
    
    def parse_date(self, value: str) -> Optional[str]:
        if not value or not value.strip():
            return None
        
        value = value.strip()
        
        for fmt in DATE_FORMATS:
            try:
                parsed = datetime.strptime(value, fmt).date()
                if 1900 <= parsed.year <= 2100:
                    return parsed.isoformat()
            except ValueError:
                continue
        
        # Handle ordinal dates
        ordinal_match = re.search(r"(\w+)\s+(\d+)(?:st|nd|rd|th)\s+(\d{4})", value)
        if ordinal_match:
            month_name, day, year = ordinal_match.groups()
            try:
                reformed = f"{month_name} {day} {year}"
                parsed = datetime.strptime(reformed, "%B %d %Y").date()
                return parsed.isoformat()
            except ValueError:
                pass
        
        return None
    
    def clean_date(self, value: str, row: int) -> str:
        result = self.parse_date(value)
        if result is None and value.strip():
            self.log_issue(row, "move_in_date", value, "invalid date format")
            return ""
        return result or ""
    
    def clean_boolean(self, value: str, row: int) -> str:
        if not value or not value.strip():
            return ""
        
        cleaned = value.strip().lower()
        if cleaned in BOOLEAN_TRUE:
            return "1"
        elif cleaned in BOOLEAN_FALSE:
            return "0"
        else:
            self.log_issue(row, "active_status", value, "unknown boolean value")
            return ""
    
    def parse_currency(self, value: str) -> int:
        if not value or not value.strip():
            return 0
        
        value = str(value).strip()
        is_negative = value.startswith("(") and value.endswith(")")
        if is_negative:
            value = value[1:-1]
        
        for symbol in CURRENCY_SYMBOLS:
            value = value.replace(symbol, "")
        
        value = value.replace(" ", "").replace(",", "")
        
        if "." not in value and "," in value:
            value = value.replace(",", ".")
        elif "," in value and "." in value:
            value = value.replace(",", "")
        
        if re.match(r"^\d+\.?\d*e\d+$", value, re.IGNORECASE):
            try:
                cents = int(round(float(value) * 100))
                return -cents if is_negative else cents
            except ValueError:
                return 0
        
        if value.lower().endswith("k"):
            try:
                cents = int(round(float(value[:-1]) * 1000 * 100))
                return -cents if is_negative else cents
            except ValueError:
                return 0
        
        try:
            cents = int(round(float(value) * 100))
            return -cents if is_negative else cents
        except ValueError:
            return 0
    
    def clean_currency(self, value: str, _: int) -> int:
        result = self.parse_currency(value)
        return result
    
    def clean_email(self, value: str, row: int) -> str:
        if not value or not value.strip():
            return ""
        
        email = value.strip().lower()
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        
        if not re.match(pattern, email):
            self.log_issue(row, "email", value, "invalid email format")
            return ""
        
        return email
    
    def clean_phone(self, value: str, row: int) -> str:
        if not value or not value.strip():
            return ""
        
        digits = re.sub(r"\D", "", value)
        
        if len(digits) < 10 or len(digits) > 15:
            self.log_issue(row, "phone", value, "invalid phone length")
            return ""
        
        return digits
    
    def clean_company_id(self, value: str, _: int) -> str:
        if not value or not value.strip():
            return ""
        
        cleaned = value.strip().upper()
        
        if cleaned.isdigit():
            cleaned = f"COMP{cleaned.zfill(3)}"
        elif not cleaned.startswith("COMP"):
            digits = re.sub(r"\D", "", cleaned)
            if digits:
                cleaned = f"COMP{digits.zfill(3)}"
        
        return cleaned
    
    def process_row(self, row: Dict[str, str], row_num: int) -> Dict[str, Any]:
        record_id = self.clean_id(row.get("rec_id", ""), row_num)
        client_name = self.clean_name(row.get("clientFullName", ""), row_num)
        
        cleaned = {
            "record_id": record_id,
            "client_name": client_name,
            "unit_number": self.clean_unit(row.get("unit", ""), row_num),
            "move_in_date": self.clean_date(row.get("moveIn", ""), row_num),
            "active_status": self.clean_boolean(row.get("isActive", ""), row_num),
            "balance_cents": self.clean_currency(row.get("balance", ""), row_num),
            "email": self.clean_email(row.get("email", ""), row_num),
            "phone": self.clean_phone(row.get("phone", ""), row_num),
            "company_id": self.clean_company_id(row.get("company_id", ""), row_num)
        }
        
        if record_id and client_name:
            self.stats['valid'] += 1
        
        return cleaned
    
    def generate_field_mappings(self) -> List[Dict[str, str]]:
        return [
            {"legacy_field": "rec_id", "target_field": "record_id", 
             "transformation": "Extract digits, validate uniqueness", "data_type": "integer", 
             "example": "A1003 → 1003"},
            {"legacy_field": "clientFullName", "target_field": "client_name", 
             "transformation": "Trim whitespace, normalize spaces", "data_type": "string", 
             "example": "  John  O'Neil  → John O'Neil"},
            {"legacy_field": "unit", "target_field": "unit_number", 
             "transformation": "Uppercase, remove special chars", "data_type": "string", 
             "example": "#4B → 4B"},
            {"legacy_field": "moveIn", "target_field": "move_in_date", 
             "transformation": "Parse multiple formats to ISO 8601", "data_type": "date", 
             "example": "03/15/24 → 2024-03-15"},
            {"legacy_field": "isActive", "target_field": "active_status", 
             "transformation": "Convert to binary 1/0", "data_type": "boolean", 
             "example": "Yes → 1, False → 0"},
            {"legacy_field": "balance", "target_field": "balance_cents", 
             "transformation": "Parse currency to integer cents", "data_type": "integer", 
             "example": "$1,200.50 → 120050"},
            {"legacy_field": "email", "target_field": "email", 
             "transformation": "Validate and normalize", "data_type": "string", 
             "example": "Jane.Smith@EMAIL.com → jane.smith@email.com"},
            {"legacy_field": "phone", "target_field": "phone", 
             "transformation": "Extract digits only", "data_type": "string", 
             "example": "(555) 123-4567 → 5551234567"},
            {"legacy_field": "company_id", "target_field": "company_id", 
             "transformation": "Standardize to COMP### format", "data_type": "string", 
             "example": "001 → COMP001"}
        ]
    
    def generate_html_report(self, output_dir: Path, cleaned_data: List[Dict]) -> str:
        total = self.stats['total']
        valid = self.stats['valid']
        success_rate = (valid / total * 100) if total > 0 else 0
        
        issue_types = Counter(issue[3].split(':')[0] for issue in self.issues)
        
        issue_rows = ""
        for issue_type, count in issue_types.most_common():
            issue_rows += f"<tr><td>{html.escape(issue_type)}</td><td>{count}</td></tr>"
        
        mapping_rows = ""
        for mapping in self.generate_field_mappings():
            mapping_rows += f"""
            <tr>
                <td><code>{html.escape(mapping['legacy_field'])}</code></td>
                <td><code>{html.escape(mapping['target_field'])}</code></td>
                <td>{html.escape(mapping['transformation'])}</td>
                <td><span class="data-type">{html.escape(mapping['data_type'])}</span></td>
                <td><code>{html.escape(mapping['example'])}</code></td>
            </tr>"""
        
        sample_rows = ""
        for row in cleaned_data[:10]:
            active_text = 'Active' if row.get('active_status') == '1' else 'Inactive' if row.get('active_status') == '0' else 'Unknown'
            sample_rows += f"""
            <tr>
                <td>{row.get('record_id', 'N/A')}</td>
                <td>{html.escape(str(row.get('client_name', '')))}</td>
                <td>{html.escape(str(row.get('unit_number', '')))}</td>
                <td>{html.escape(str(row.get('move_in_date', '')))}</td>
                <td>{active_text}</td>
                <td>${(row.get('balance_cents', 0) / 100):.2f}</td>
            </tr>"""
        
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Cleanup Report</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; border-bottom: 3px solid #007acc; padding-bottom: 20px; margin-bottom: 30px; }}
        h1 {{ color: #333; margin: 0; font-size: 2.5em; }}
        .subtitle {{ color: #666; font-size: 1.1em; margin-top: 10px; }}
        .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 30px 0; }}
        .metric {{ background: linear-gradient(135deg, #007acc, #0056b3); color: white; padding: 20px; border-radius: 8px; text-align: center; }}
        .metric-value {{ font-size: 2em; font-weight: bold; display: block; }}
        .metric-label {{ font-size: 0.9em; opacity: 0.9; margin-top: 5px; }}
        .section {{ margin: 40px 0; }}
        .section h2 {{ color: #333; border-left: 4px solid #007acc; padding-left: 15px; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; background: white; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #f8f9fa; font-weight: 600; color: #333; }}
        tr:hover {{ background-color: #f8f9fa; }}
        code {{ background: #f1f3f4; padding: 2px 6px; border-radius: 3px; font-family: 'Consolas', monospace; font-size: 0.9em; }}
        .data-type {{ background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 12px; font-size: 0.8em; font-weight: 500; }}
        .success {{ color: #4caf50; font-weight: bold; }}
        .footer {{ text-align: center; margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Data Cleanup Report</h1>
            <div class="subtitle">Legacy Data Transformation Analysis</div>
            <div class="subtitle">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
        
        <div class="metrics">
            <div class="metric">
                <span class="metric-value">{total:,}</span>
                <div class="metric-label">Total Records</div>
            </div>
            <div class="metric">
                <span class="metric-value">{valid:,}</span>
                <div class="metric-label">Valid Records</div>
            </div>
            <div class="metric">
                <span class="metric-value">{success_rate:.1f}%</span>
                <div class="metric-label">Success Rate</div>
            </div>
            <div class="metric">
                <span class="metric-value">{len(self.issues):,}</span>
                <div class="metric-label">Issues Handled</div>
            </div>
        </div>
        
        <div class="section">
            <h2>Field Transformations</h2>
            <table>
                <thead>
                    <tr><th>Legacy Field</th><th>Target Field</th><th>Transformation</th><th>Data Type</th><th>Example</th></tr>
                </thead>
                <tbody>{mapping_rows}</tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>Issue Summary</h2>
            <table>
                <thead>
                    <tr><th>Issue Type</th><th>Count</th></tr>
                </thead>
                <tbody>{issue_rows}</tbody>
            </table>
        </div>
        
        <div class="section">
            <h2>Sample Data Preview</h2>
            <table>
                <thead>
                    <tr><th>ID</th><th>Name</th><th>Unit</th><th>Move-in Date</th><th>Status</th><th>Balance</th></tr>
                </thead>
                <tbody>{sample_rows}</tbody>
            </table>
        </div>
        
        <div class="footer">
            <p><strong>Professional Data Cleanup Service</strong></p>
            <p>Contact: salalalikhan@gmail.com</p>
        </div>
    </div>
</body>
</html>"""
        
        report_path = output_dir / "report.html"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return str(report_path)


def parse_args():
    parser = argparse.ArgumentParser(description="Data cleanup tool for legacy CSV exports")
    parser.add_argument("--input", "-i", required=True, help="Input CSV file")
    parser.add_argument("--schema", "-s", help="Schema CSV file")
    parser.add_argument("--output", "-o", required=True, help="Output directory")
    parser.add_argument("--format", "-f", choices=["csv", "json", "both"], default="both", help="Output format")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    return parser.parse_args()


def main():
    args = parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    input_file = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return 1
    
    logger.info(f"Processing {input_file}")
    
    processor = DataProcessor()
    processor.processing_times['start_time'] = time.time()
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            raw_data = list(reader)
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        return 1
    
    processor.stats['total'] = len(raw_data)
    logger.info(f"Loaded {len(raw_data)} rows")
    
    cleaned_data = []
    for i, row in enumerate(raw_data, 1):
        cleaned_row = processor.process_row(row, i)
        cleaned_data.append(cleaned_row)
    
    fieldnames = ["record_id", "client_name", "unit_number", "move_in_date", 
                  "active_status", "balance_cents", "email", "phone", "company_id"]
    
    if args.format in ["csv", "both"]:
        csv_output = output_dir / "cleaned_import.csv"
        with open(csv_output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_data)
        logger.info(f"CSV output: {csv_output}")
    
    if args.format in ["json", "both"]:
        json_output = output_dir / "cleaned_import.json"
        with open(json_output, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
        logger.info(f"JSON output: {json_output}")
    
    issues_output = output_dir / "data_quality_issues.csv"
    with open(issues_output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["row_number", "field", "raw_value", "issue_description"])
        writer.writerows(processor.issues)
    logger.info(f"Issues report: {issues_output}")
    
    mappings = processor.generate_field_mappings()
    mapping_output = output_dir / "field_mapping.csv"
    with open(mapping_output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["legacy_field", "target_field", "transformation", "data_type", "example"])
        writer.writeheader()
        writer.writerows(mappings)
    logger.info(f"Field mappings: {mapping_output}")
    
    html_report = processor.generate_html_report(output_dir, cleaned_data)
    logger.info(f"HTML report: {html_report}")
    
    summary_output = output_dir / "summary.txt"
    with open(summary_output, 'w', encoding='utf-8') as f:
        stats = processor.stats
        success_rate = (stats['valid'] / stats['total'] * 100) if stats['total'] > 0 else 0
        f.write("DATA CLEANUP SUMMARY\n")
        f.write("=" * 20 + "\n\n")
        f.write(f"Total records: {stats['total']:,}\n")
        f.write(f"Valid records: {stats['valid']:,}\n")
        f.write(f"Success rate: {success_rate:.1f}%\n")
        f.write(f"Issues handled: {len(processor.issues)}\n\n")
        f.write("Data cleanup completed successfully.\n")
    
    logger.info(f"Summary: {summary_output}")
    
    # Calculate performance metrics
    processor.processing_times['end_time'] = time.time()
    processing_duration = processor.processing_times['end_time'] - processor.processing_times['start_time']
    processor.processing_times['records_per_second'] = len(raw_data) / processing_duration if processing_duration > 0 else 0
    
    success_rate = processor.stats['valid'] / len(raw_data) * 100
    logger.info(f"Cleanup completed - {success_rate:.1f}% success rate")
    logger.info(f"Performance: {processor.processing_times['records_per_second']:.1f} records/second")
    logger.info(f"Total time: {processing_duration:.2f} seconds")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
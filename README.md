# Enterprise Data Cleanup Tool

A **Data transformation pipeline** designed for enterprise legacy system migration with comprehensive security, compliance and performance monitoring capabilities.

## Enterprise Features

- **Data Security & Compliance**: PII detection, automatic masking, GDPR/HIPAA compliance features
- **Performance & Scalability**: Batch processing, multi-threading, memory monitoring for million+ record datasets
- **Error Recovery**: Automatic retry logic, circuit breakers, quarantine system for problematic records
- **Advanced Analytics**: Professional Excel workbooks with 22+ formulas, pivot tables, and executive dashboards
- **Data Validation**: Schema enforcement, business rule validation, international format support
- **Performance Monitoring**: Real-time metrics, resource usage tracking, optimization recommendations
- **Configuration Management**: Environment-specific configs, hot-reloading, validation
- **Audit & Compliance**: Complete audit trails, security logging, regulatory reporting

## Quick Start

```bash
# Enterprise demonstration with all features
python demo_enterprise.py

# Basic demonstration (legacy compatibility)
python demo.py

# Enterprise processing with full monitoring
python src/enterprise_processor.py

# Individual components:
python src/data_cleanup.py --input data/input/legacy_export.csv --schema config/import_schema.csv --output outputs/
python src/excel_analysis.py
python validate_project.py
```

## Project Structure

```
enterprise-data-cleanup/
├── README.md                           # This comprehensive guide
├── demo.py                            # Basic demonstration
├── demo_enterprise.py                 # Enterprise demonstration
├── validate_project.py                # Project validation tests
├── src/                               # Enterprise source code
│   ├── data_cleanup.py               # Core data cleanup engine
│   ├── enterprise_processor.py       # Enterprise processing engine
│   ├── data_validator.py             # Schema validation & business rules
│   ├── error_handler.py              # Error recovery & retry logic
│   ├── data_security.py              # PII detection & masking
│   ├── performance_monitor.py        # Performance & batch processing
│   ├── config_manager.py             # Configuration management
│   ├── excel_analysis.py             # Excel workbook generator
│   └── excel_formulas.py             # Advanced Excel features
├── data/
│   ├── input/                        # Raw input files
│   │   └── legacy_export.csv         # Sample legacy data (500 records)
│   └── sample/                       # Sample data generators
│       └── generate_large_dataset.py
├── config/                           # Enterprise configuration
│   ├── import_schema.csv             # Target schema definition
│   ├── validation_schema.csv         # Validation rules
│   └── processing_config.json        # Enterprise settings
├── outputs/                          # Generated outputs & reports
│   ├── cleaned_import.csv            # Clean CSV output
│   ├── cleaned_import.json           # JSON output
│   ├── data_analysis_workbook.xlsx   # Advanced Excel analytics
│   ├── enterprise_*.json             # Enterprise reports
│   ├── security_audit/               # Security audit logs
│   ├── performance_logs/             # Performance metrics
│   └── quarantine/                   # Quarantined records
```

## Data Transformations

The tool handles complex data cleaning scenarios:

| Field | Transformation | Example |
|-------|---------------|---------|
| **Record ID** | Extract numeric portion | `"A1003"` → `1003` |
| **Client Name** | Normalize spacing, remove trailing commas | `"Singh, Amar"` → `Singh Amar` |
| **Unit Number** | Standardize alphanumeric format | `"4 b"` → `4B` |
| **Move-in Date** | Convert to ISO 8601 | `"Mar 17 2024"` → `2024-03-17` |
| **Active Status** | Standardize boolean values | `"Active"` → `1` |
| **Balance** | Convert to integer cents | `"$1,200.50"` → `120050` |

## International Support

- **Date Formats**: Chinese (2024年4月7日), European (31/03/2024), ordinals (April 8th 2024)
- **Currencies**: USD, EUR, GBP, JPY, ILS, RUB with scientific notation support
- **Text Encoding**: Full UTF-8 support with emoji and international characters

## Usage Examples

### Basic Cleanup
```bash
python src/data_cleanup.py \
    --input data/input/your_file.csv \
    --schema config/import_schema.csv \
    --output outputs/ \
    --verbose
```

### Generate Sample Data
```bash
python data/sample/generate_large_dataset.py
```

### Create Excel Analysis
```bash
python src/excel_analysis.py
python src/excel_formulas.py
```

## Output Quality

**Before (Legacy Data):**
```csv
A1003,"Singh, Amar",4 b,Mar 17 2024,1,1.2k,Promo benefit
,Missing ID,Unit 8,2024-03-22,Y,1000,No record id
```

**After (Clean Data):**
```csv
record_id,client_name,unit_number,move_in_date,active_status,balance_cents
1003,Singh Amar,4B,2024-03-17,1,120000
,Missing ID,UNIT8,2024-03-22,1,100000
```

## Requirements

- Python 3.8+
- pandas
- openpyxl

## Testing

Run the comprehensive validation suite:

```bash
python validate_project.py
```

This validates:
- File structure integrity
- Data processing accuracy
- Excel workbook generation
- Output format compliance
- Performance metrics

## Contact

**Salal Khan**  
📧 Email: salalalikhan@gmail.com  
---





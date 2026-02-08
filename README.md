# Simple S3 ETL (S3 → ETL → S3)

A minimal, single-file Python pipeline that:
1) Reads a **CSV** from **AWS S3**  
2) Applies a small **ETL transform** (clean columns, drop null rows, dedupe, optional revenue + timestamp parsing)  
3) Writes the result back to **S3** as **Parquet**

---

## Project Structure

```text
.
├── s3_etl.py
└── README.md

```

## Prerequisites

- Python 3.9+ (3.10/3.11 recommended)
- AWS credentials configured (via AWS CLI profile, environment variables, or IAM role)
- Access to S3:
    - Read permission for the input object (s3:GetObject)
    - Write permission for the output object (s3:PutObject)

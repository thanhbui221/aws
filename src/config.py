import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Config:
    aws_region: str = os.getenv("AWS_REGION", "ap-southeast-1")
    aws_profile: str | None = os.getenv("AWS_PROFILE")

    s3_input_uri: str = os.environ["S3_INPUT_URI"]
    s3_output_uri: str = os.environ["S3_OUTPUT_URI"]

    output_partitioned: bool = os.getenv("OUTPUT_PARTITIONED", "false").lower() == "true"

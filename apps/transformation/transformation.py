import logging
from pyspark.sql import SparkSession
from db_utils import fetch_jobs_for_transformation, update_job_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("credit-card-transformation") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

jobs = fetch_jobs_for_transformation(limit=5)
logger.info(f"Fetched {len(jobs)} jobs for transformation")

for job in jobs:
    job_id = job["job_id"]
    raw_path = job["raw_path"]

    logger.info(f"Transforming job {job_id} with raw data at {raw_path}")

    processed_path = f"/data/processed/credit_card/job_id={job_id}"
    df = spark.read.option("header", True).csv(raw_path)
    df = df.filter(df["txn_amount"].cast("decimal") > 50000.00)
    df.write.mode("overwrite").csv(processed_path)
    update_job_status(job_id, "transformed", processed_path)
    logger.info(f"Job {job_id} transformed successfully and written to {processed_path}")
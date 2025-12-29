import logging
import os
import glob
from datetime import datetime
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



def cleanup_temp_files(output_path):
    try :
        folder_path = output_path
        logger.info(f"Cleaning up temporary files in {folder_path}...")

        # removing all non csv files
        for fname in os.listdir(folder_path):
            nonCSV_file = os.path.join(folder_path, fname)
            if os.path.isfile (nonCSV_file) and not fname.endswith('.csv'):
                os.remove(nonCSV_file)
                logger.info(f"Removed temporary file: {nonCSV_file}") 
        
        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        if not csv_files:
            logger.warning("No CSV files found to rename.")
            return
        if len(csv_files) > 1:
            logger.warning("Multiple CSV files found. Renaming the first one found.")
        
        old_csv = csv_files[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"credit_card_spent_{timestamp}.csv"
        new_path = os.path.join(folder_path, new_name)
        os.rename(old_csv, new_path)
    except Exception as e:
        logger.error(f"Error during cleanup of temporary files: {e}")
# etl_pipeline
This is a local PySpark ETL (Extract-Transform-Load) system running in Docker Compose, designed for processing credit card transaction data.

## Base commands to follow
- build docker images in demon mode.
docker-compose up --build -d 
docker-compose ps

- check postgres table
docker-compose exec postgres psql -U postgres -d etl_db -c "select * from job_metadata"
 
- check logs of extraction app
docker logs -f extraction-app  -- show on terminal
docker logs -f extraction-app > extractions.log -- create a log file names extraction.log


- check logs of transformation app
docker logs -f transformation-app  -- show on terminal
docker logs -f transformation-app > transformation.log -- create a log file names transformation.log

terminate all images
--------------------
docker-compose down -v

remove all images
--------------------
docker system prune -f
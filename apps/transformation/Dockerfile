FROM apache/spark:3.5.3
USER root
RUN pip install psycopg2-binary pandas openpyxl xlsxwriter
WORKDIR /apps/transformation
COPY db_utils.py transformation.py ./

FROM apache/spark:3.5.3
USER root
RUN pip install psycopg2-binary faker
WORKDIR /apps/extraction
COPY db_utils.py extraction.py user.txt account.txt card.txt merch.txt credit_card_spent.json ./

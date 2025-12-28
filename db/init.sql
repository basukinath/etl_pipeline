CREATE TABLE IF NOT EXISTS job_metadata (
    job_id text PRIMARY KEY,
    job_name VARCHAR(50),
    status VARCHAR(20),
    raw_path TEXT,
    processed_path TEXT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    error_message TEXT
);
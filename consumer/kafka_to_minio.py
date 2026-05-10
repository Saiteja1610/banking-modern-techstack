import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load secrets from .env
load_dotenv()

# Verify environment variables
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP")
logger.info(f"🔍 KAFKA_BOOTSTRAP: {kafka_bootstrap}")

# Kafka consumer settings
try:
    logger.info(f"📡 Connecting to Kafka at {kafka_bootstrap}...")
    consumer = KafkaConsumer(
        'banking_server.public.customers',
        'banking_server.public.accounts',
        'banking_server.public.transactions',
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=os.getenv("KAFKA_GROUP"),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        session_timeout_ms=30000,
        request_timeout_ms=60000
    )
    logger.info("✅ Connected to Kafka successfully")
except Exception as e:
    logger.error(f"❌ Failed to connect to Kafka: {e}", exc_info=True)
    exit(1)

# MinIO client
try:
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )

    bucket = os.getenv("MINIO_BUCKET")

    # Verify MinIO connection
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    logger.info(f"✅ MinIO connected. Existing buckets: {buckets}")
    
    if bucket not in buckets:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"✅ Created bucket: {bucket}")
    else:
        logger.info(f"✅ Using existing bucket: {bucket}")
except Exception as e:
    logger.error(f"❌ Failed to connect to MinIO: {e}", exc_info=True)
    exit(1)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    try:
        df = pd.DataFrame(records)
        date_str = datetime.now().strftime('%Y-%m-%d')
        file_path = f'{table_name}_{date_str}.parquet'
        
        # Write to local parquet file
        df.to_parquet(file_path, engine='fastparquet', index=False)
        logger.info(f"📝 Created local file: {file_path} ({len(records)} records, {df.memory_usage(deep=True).sum() / 1024:.2f}KB)")
        
        # Upload to MinIO
        s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
        s3.upload_file(file_path, bucket, s3_key)
        logger.info(f"✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}")
        
        # Cleanup
        os.remove(file_path)
        logger.info(f"🗑️  Cleaned up local file: {file_path}")
    except Exception as e:
        logger.error(f"❌ Error uploading to MinIO: {e}", exc_info=True)

# Batch consume
batch_size = 50
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")
logger.info("🚀 Starting Kafka consumer...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = payload.get("after")  # Only take the actual row

    if record:
        buffer[topic].append(record)
        logger.debug(f"[{topic}] Received record: {record}")
        
        batch_count = len(buffer[topic])
        logger.info(f"📊 [{topic}] Buffer: {batch_count}/{batch_size} records")

        if batch_count >= batch_size:
            logger.info(f"🔄 Batch full for {topic}, uploading {batch_size} records...")
            write_to_minio(topic.split('.')[-1], buffer[topic])
            buffer[topic] = []
    else:
        logger.warning(f"⚠️  No 'after' field in payload for topic {topic}")
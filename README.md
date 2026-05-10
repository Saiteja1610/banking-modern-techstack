# 🏦 Banking Modern Data Stack

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-EF3B2D?logo=apache&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?logo=git&logoColor=white)
![CI/CD](https://img.shields.io/badge/CI%2FCD-000000?logo=githubactions&logoColor=white)

---

## 📌 Project Overview

### **🎯 Mission & Vision**
This project demonstrates a **production-ready, end-to-end modern data stack pipeline** for the **Banking & Financial Services domain**. We simulate a complete **banking data ecosystem** that handles customer relationships, account management, and transaction processing - showcasing how modern data engineering practices can transform traditional banking operations into real-time, analytics-driven insights.

### **💡 Business Value**
- **Real-Time Customer Insights**: Track customer behavior, account activity, and transaction patterns in real-time
- **Regulatory Compliance**: Maintain audit trails and historical data for compliance reporting
- **Operational Efficiency**: Automate data pipelines for reduced manual intervention and faster insights
- **Scalable Analytics**: Support growing data volumes with cloud-native architecture

### **🔧 Technical Scope**
This implementation covers the complete **data lifecycle** from source systems to business intelligence:

**Data Ingestion & Streaming:**
- Synthetic banking data generation using Python Faker
- PostgreSQL OLTP database with ACID transactions
- Change Data Capture (CDC) via Debezium + Kafka
- Real-time streaming to object storage (MinIO)

**Data Processing & Transformation:**
- Orchestrated ETL pipelines using Apache Airflow
- Cloud data warehousing with Snowflake (Bronze → Silver → Gold)
- Data modeling and transformation with DBT
- Slowly Changing Dimensions (SCD Type-2) for historical tracking

**Quality & Reliability:**
- Automated testing and data validation
- CI/CD pipelines with GitHub Actions
- Containerized deployment with Docker
- Comprehensive monitoring and logging

### **🎓 Learning Objectives**
This project serves as a **comprehensive learning platform** for:
- **Data Engineering**: Building scalable data pipelines and architectures
- **Real-Time Processing**: Implementing CDC and streaming architectures
- **Cloud Data Warehousing**: Designing multi-layer data architectures
- **Data Modeling**: Creating analytics-ready dimensional models
- **DevOps Practices**: Implementing CI/CD for data projects
- **Infrastructure as Code**: Managing complex multi-service environments

### **🏆 Key Achievements**
- **Complete Data Ecosystem**: From OLTP source to analytics-ready warehouse
- **Real-Time Capabilities**: Sub-second data latency from source to consumption
- **Production Patterns**: Enterprise-grade architecture with proper error handling
- **Cost Optimization**: Efficient resource utilization and scalable compute
- **Maintainability**: Well-documented, tested, and automated codebase
- **Industry Best Practices**: Following modern data engineering standards

### **📊 Business Impact Demonstration**
Imagine a modern bank that needs to:
- **Monitor account activity** in real-time for fraud prevention
- **Analyze customer behavior** across multiple touchpoints
- **Generate regulatory reports** with complete audit trails
- **Support data scientists** with clean, transformed datasets
- **Scale operations** as customer base grows

This project provides exactly that foundation - a **blueprint for digital transformation** in financial services.

### **🚀 Innovation Highlights**
- **Event-Driven Architecture**: Real-time data streaming vs. traditional batch processing
- **Cloud-Native Design**: Scalable, resilient infrastructure using containerization
- **Data Mesh Principles**: Domain-oriented data products with clear ownership
- **Infrastructure Automation**: GitOps-driven deployment and configuration management
- **Observability-First**: Comprehensive monitoring and alerting for operational excellence

👉 **Think of it as a real-world banking data ecosystem** built on modern cloud-native tools, demonstrating how traditional financial institutions can leverage data engineering to drive digital transformation and competitive advantage.

---

## 🏗️ Architecture  

<img width="5647" height="3107" alt="Architecture" src="https://github.com/user-attachments/assets/7521ea8a-451e-46ff-9db0-71dd6ddf8181" />


**Pipeline Flow:**
1. **Data Generator** → Simulates banking transactions, accounts & customers (via Faker).  
2. **Kafka + Debezium** → Streams change data (CDC) into MinIO (S3-compatible storage).  
3. **Airflow** → Orchestrates data ingestion & snapshots into Snowflake.  
4. **Snowflake** → Cloud Data Warehouse (Bronze → Silver → Gold).  
5. **DBT** → Applies transformations, builds marts & snapshots (SCD Type-2).  
6. **CI/CD with GitHub Actions** → Automated tests, build & deployment.  

---

## 🏛️ Detailed Architecture Components

### **1. Data Source Layer (PostgreSQL OLTP)**
- **Technology**: PostgreSQL 15 with logical replication enabled
- **Schema Design**: Normalized relational model with foreign key constraints
- **Tables**: `customers`, `accounts`, `transactions` with proper indexing
- **CDC Setup**: WAL (Write-Ahead Log) level logical replication for Debezium
- **Data Volume**: Configurable synthetic data generation (1000+ customers, accounts, transactions)

### **2. Streaming Layer (Kafka + Debezium)**
- **Kafka Broker**: Single-node setup with Zookeeper for coordination
- **Debezium Connector**: PostgreSQL source connector with Avro serialization
- **Topics**: `banking_server.public.customers`, `banking_server.public.accounts`, `banking_server.public.transactions`
- **Consumer Group**: `minio-landing-group` for fault-tolerant message processing
- **Message Format**: JSON with `before`/`after` states, operation types (INSERT/UPDATE/DELETE)

### **3. Storage Layer (MinIO)**
- **S3-Compatible**: API-compatible with AWS S3 for seamless integration
- **Bucket Structure**: `rawdata` bucket with partitioned paths (`table/date=YYYY-MM-DD/`)
- **File Format**: Parquet for columnar storage and compression
- **Partitioning**: Date-based partitioning for efficient querying
- **Consumer**: Python Kafka consumer with batching (50 records) and error handling

### **4. Orchestration Layer (Apache Airflow)**
- **DAGs**: `minio_to_snowflake_dag.py` for scheduled data ingestion
- **Scheduling**: Configurable intervals (hourly/daily) with catchup enabled
- **Tasks**: 
  - MinIO file discovery and listing
  - Snowflake stage creation and data loading
  - DBT model execution and testing
- **Dependencies**: Task dependencies with proper error handling and retries

### **5. Data Warehouse Layer (Snowflake)**
- **Architecture**: Multi-cluster, shared-data architecture
- **Databases**: `BANKING` database with separate schemas
- **Schema Layers**:
  - **Bronze**: Raw ingested data from MinIO
  - **Silver**: Cleaned and transformed staging data
  - **Gold**: Business-ready marts (facts, dimensions)
- **Compute**: Virtual warehouses for isolated workloads
- **Storage**: Automatic clustering and micro-partitioning

### **6. Transformation Layer (DBT)**
- **Project Structure**: Modular with staging → marts progression
- **Models**:
  - **Staging**: `stg_customers`, `stg_accounts`, `stg_transactions`
  - **Marts**: Dimension tables (`dim_customers`, `dim_accounts`) and fact tables (`fact_transactions`)
- **Snapshots**: SCD Type-2 for `customers` and `accounts` tables
- **Testing**: Data quality tests (uniqueness, not_null, relationships)
- **Materialization**: Incremental models for performance optimization

### **7. CI/CD Layer (GitHub Actions)**
- **Workflows**: 
  - **CI**: Linting, testing, dbt compilation on PR/push
  - **CD**: Automated deployment on merge to main
- **Secrets Management**: GitHub Secrets for Snowflake, Postgres credentials
- **Artifact Storage**: Docker images and build artifacts
- **Environments**: Dev/Staging/Prod with proper approvals

---

## 🔄 Data Flow Architecture

### **Real-Time Data Pipeline**
```
PostgreSQL → Debezium → Kafka → Python Consumer → MinIO (Parquet)
```

1. **Change Detection**: Debezium monitors PostgreSQL WAL for changes
2. **Event Publishing**: Changes published to Kafka topics with schema information
3. **Message Consumption**: Python consumer batches messages (50 records)
4. **Data Serialization**: Records converted to Parquet format with partitioning
5. **Object Storage**: Files uploaded to MinIO with structured paths

### **Batch Processing Pipeline**
```
MinIO → Airflow → Snowflake (Bronze) → DBT → Snowflake (Silver/Gold)
```

1. **File Discovery**: Airflow DAG scans MinIO for new Parquet files
2. **Data Loading**: Files staged and loaded into Snowflake Bronze layer
3. **Transformation**: DBT models transform Bronze → Silver → Gold
4. **Quality Assurance**: Automated testing and validation
5. **Snapshot Updates**: SCD Type-2 snapshots for historical tracking

### **Data Model Flow**
- **Source**: Normalized OLTP schema (3NF)
- **Staging**: Denormalized for analytics (light transformations)
- **Marts**: Star schema with conformed dimensions and facts
- **Snapshots**: Historical tracking with effective dates

---

## 🐳 Infrastructure Architecture

### **Container Orchestration (Docker Compose)**
- **Services**: 8 interconnected containers with health checks
- **Networking**: Isolated bridge networks with service discovery
- **Volumes**: Persistent data storage for databases and MinIO
- **Dependencies**: Service startup order with health check dependencies

### **Service Configuration**
```yaml
# Key services and their roles:
- postgres: OLTP database with CDC
- kafka + zookeeper: Message streaming
- debezium: CDC connector
- minio: Object storage
- airflow: Workflow orchestration
- data-generator: Synthetic data creation
```

### **Resource Allocation**
- **Memory**: Configured limits and reservations per service
- **CPU**: Appropriate CPU shares for compute-intensive services
- **Storage**: Volume mounts for data persistence
- **Ports**: Internal/external port mappings for access

---

### **Warehouse Schema (Snowflake)**
- **Bronze Layer**: Raw Parquet files loaded as external tables
- **Silver Layer**: Cleaned staging tables with data quality checks
- **Gold Layer**: 
  - `dim_customers`: Customer dimension with SCD Type-2
  - `dim_accounts`: Account dimension with SCD Type-2  
  - `fact_transactions`: Transaction facts with foreign keys

### **Snapshot Strategy**
- **SCD Type-2**: Full historical tracking for customers and accounts
- **Effective Dates**: `effective_from`, `effective_to`, `is_current` columns
- **Change Tracking**: Surrogate keys for dimension versioning

---

## 🔒 Security Architecture

### **Authentication & Authorization**
- **Database Security**: Role-based access control in Snowflake
- **API Security**: MinIO with access/secret keys
- **Container Security**: Non-root users, minimal base images
- **Network Security**: Internal networking, no external exposure

### **Secrets Management**
- **GitHub Secrets**: Encrypted storage for cloud credentials
- **Environment Variables**: Runtime configuration without hardcoding
- **Docker Secrets**: Secure credential passing between containers
- **Access Control**: Least privilege principle across all services

### **Data Protection**
- **Encryption**: Data at rest and in transit
- **Audit Logging**: Comprehensive logging for compliance
- **Data Masking**: PII protection in development environments
- **Backup Strategy**: Automated backups with retention policies

---

## 📊 Monitoring & Observability

### **Logging Strategy**
- **Application Logs**: Structured logging with correlation IDs
- **Infrastructure Logs**: Container logs with log aggregation
- **Audit Logs**: Data access and transformation tracking
- **Error Handling**: Comprehensive error capture and alerting

### **Metrics & Monitoring**
- **Pipeline Metrics**: DAG success rates, data latency, throughput
- **System Metrics**: CPU, memory, disk usage per service
- **Data Quality**: Automated tests and data validation checks
- **Business Metrics**: Data freshness, completeness, accuracy

### **Alerting**
- **Failure Alerts**: Pipeline failures, data quality issues
- **Performance Alerts**: Resource utilization thresholds
- **SLA Monitoring**: Data delivery timeliness and completeness

---

## ⚡ Performance & Scalability

### **Optimization Strategies**
- **Data Partitioning**: Date-based partitioning in MinIO and Snowflake
- **Incremental Loading**: DBT incremental models for efficiency
- **Batch Processing**: Configurable batch sizes for optimal throughput
- **Caching**: Query result caching in Snowflake

### **Scalability Considerations**
- **Horizontal Scaling**: Kafka consumer groups for parallel processing
- **Compute Scaling**: Snowflake virtual warehouses scale automatically
- **Storage Scaling**: MinIO distributed setup for large data volumes
- **Container Scaling**: Docker Compose with resource limits

### **Resource Optimization**
- **Memory Management**: Appropriate heap sizes for JVM services
- **Connection Pooling**: Efficient database connection management
- **Query Optimization**: Optimized SQL in DBT models
- **Compression**: Parquet compression for storage efficiency

---

## 🚀 Deployment Architecture

### **Environment Strategy**
- **Development**: Local Docker Compose with hot reloading
- **Staging**: Cloud deployment with production-like configuration
- **Production**: Fully automated deployment with monitoring

### **CI/CD Pipeline**
```yaml
# GitHub Actions workflow stages:
1. Code Quality: Linting, formatting, security scanning
2. Testing: Unit tests, integration tests, dbt tests
3. Build: Docker image creation, artifact packaging
4. Deploy: Infrastructure provisioning, service deployment
5. Validation: Health checks, smoke tests, data validation
```

### **Infrastructure as Code**
- **Docker Compose**: Local development environment
- **GitHub Actions**: CI/CD automation
- **Configuration Management**: Environment-specific configurations
- **Secret Management**: Secure credential handling

---

## ⚡ Tech Stack
- **Snowflake** → Cloud Data Warehouse  
- **DBT** → Transformations, testing, snapshots (SCD Type-2)  
- **Apache Airflow** → Orchestration & DAG scheduling  
- **Apache Kafka + Debezium** → Real-time streaming & CDC  
- **MinIO** → S3-compatible object storage  
- **Postgres** → Source OLTP system  
- **Python (Faker)** → Data simulation  
- **Docker & docker-compose** → Containerized setup  
- **Git & GitHub Actions** → CI/CD workflows  

---

## ✅ Key Features
- **PostgreSQL OLTP**: Source relational database with ACID guarantees (customers, accounts, transactions)  
- **Simulated banking system**: customers, accounts, and transactions  
- **Change Data Capture (CDC)** via Kafka + Debezium (capturing Postgres WAL)  
- **Raw → Staging → Fact/Dimension** models in DBT  
- **Snapshots for history tracking** (slowly changing dimensions)  
- **Automated pipeline orchestration** using Airflow  
- **CI/CD pipeline** with dbt tests + GitHub Actions  

---

## 📂 Repository Structure
```text
banking-modern-stack/
├── .github/workflows/         # CI/CD pipelines (ci.yml, cd.yml)
├── banking_dbt/              # DBT project
│   ├── models/
│   │   ├── staging/           # Staging models
│   │   ├── marts/             # Facts & dimensions
│   │   └── sources.yml
│   ├── snapshots/             # SCD2 snapshots
│   └── dbt_project.yml
├── consumer
│   └── kafka_to_minio.py
├── data-generator/            # Faker-based data simulator
│   └── faker_generator.py
├── docker/                    # Airflow DAGs, plugins, etc.
│   ├── dags/                  # DAGs (minio_to_snowflake, scd_snapshots)
├── kafka-debezium/            # Kafka connectors & CDC logic
│   └── generate_and_post_connector.py
├── postgres/                  # Postgres schema (OLTP DDL & seeds)
│   └── schema.sql
├── .gitignore
├── docker-compose.yml         # Containerized infra
├── dockerfile-airflow.dockerfile
├── requirements.txt
└── README.md
```

---

## ⚙️ Step-by-Step Implementation  

### **1. Data Simulation**  
- Generated synthetic banking data (**customers, accounts, transactions**) using **Faker**.  
- Inserted data into **PostgreSQL (OLTP)** so the system behaves like a real transactional database (**ACID, constraints**).  
- Controlled generation via `config.yaml`.  

---

### **2. Kafka + Debezium CDC**  
- Set up **Kafka Connect & Debezium** to capture changes from **Postgres**.  
- Streamed **CDC events** into **MinIO**.  

---

### **3. Airflow Orchestration**  
- Built DAGs to:  
  - Ingest **MinIO data → Snowflake (Bronze)**.  
  - Schedule **snapshots & incremental loads**.  

---

### **4. Snowflake Warehouse**  
- Organized into **Bronze → Silver → Gold layers**.  
- Created **staging schemas** for ingestion.  

---

### **5. DBT Transformations**  
- **Staging models** → cleaned source data.  
- **Dimension & fact models** → built marts.  
- **Snapshots** → tracked history of accounts & customers.  

---

### **6. CI/CD with GitHub Actions**  
- **ci.yml** → Lint, dbt compile, run tests.  
- **cd.yml** → Deploy DAGs & dbt models on merge.  

---

## 📊 Final Deliverables  
- **Automated CDC pipeline** from Postgres → Snowflake  
- **DBT models** (facts, dimensions, snapshots)  
- **Orchestrated DAGs in Airflow**  
- **Synthetic banking dataset** for demos  
- **CI/CD workflows** ensuring reliability  




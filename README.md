# TableCheck Data Operations Assignment

## Table of Contents
1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Data Pipeline](#data-pipeline)
   - [Data Transformation (DuckDB)](#1-data-transformation-duckdb)
   - [BigQuery Setup](#2-bigquery-setup)
4. [Visualizations (Looker Studio)](#visualizations-looker-studio)
5. [How to Run the Project](#how-to-run-the-project)
7. [Additional Considerations](#additional-considerations)

## Overview
This project analyzes restaurant data to answer key business questions using DuckDB for data transformation and Looker Studio for visualization. The pipeline processes raw CSV data, performs transformations, and creates visualizations to provide insights into restaurant performance and customer behavior.

## Project Structure
```
project_root/
│
├── config/
│   └── bigquery_credentials.json
│
├── src/
│   ├── duckdb_transformation.py
│   ├── bigquery_setup.py
│   └── tmp/
│       ├── restaurant_stats.csv
│       ├── popular_dishes.csv
│       ├── profitable_dishes.csv
│       ├── customer_stats.csv
│       └── frequent_visitors.csv
│
├── data/
│   └── data.csv
│
└── pyproject.toml
```

## Data Pipeline

### 1. Data Transformation (DuckDB)
File: `src/duckdb_transformation.py`

This script performs the following operations:
- Loads raw data from CSV
- Cleans and transforms the data
- Creates aggregated tables for analysis
- Exports transformed data to CSV files in the `src/tmp/` directory

Key functions:
- `clean_and_transform_data()`: Cleans the raw data
- `create_restaurant_stats()`: Aggregates restaurant-level statistics
- `create_popular_dishes()`: Identifies top 5 popular dishes per restaurant
- `create_profitable_dishes()`: Identifies top 5 profitable dishes per restaurant
- `create_customer_stats()`: Analyzes customer visit patterns
- `create_frequent_visitors()`: Identifies the most frequent visitor for each restaurant

### 2. BigQuery Setup
File: `src/bigquery_setup.py`

This script:
- Connects to BigQuery
- Creates necessary tables
- Loads transformed data from CSV files (located in `src/tmp/`) into BigQuery

## Visualizations (Looker Studio)

The dashboard for this project can be accessed at the following URL:
[Looker Studio Dashboard](https://lookerstudio.google.com/reporting/406a761e-2ee9-4709-b732-cfd86eb757e6)

The Looker Studio dashboard answers the following questions:

1. How many customers visited the "Restaurant at the end of the universe"?
   - Chart Name: "End of Universe Restaurant - Customer Count"
   - Type: Scorecard
   - Data source: restaurant_stats table

2. How much money did the "Restaurant at the end of the universe" make?
   - Chart Name: "End of Universe Restaurant - Total Revenue"
   - Type: Scorecard
   - Data source: restaurant_stats table

3. What was the most popular dish at each restaurant?
   - Chart Name: "Most Popular Dishes by Restaurant"
   - Type: Table
   - Data source: popular_dishes table

4. What was the most profitable dish at each restaurant?
   - Chart Name: "Most Profitable Dishes by Restaurant"
   - Type: Table
   - Data source: profitable_dishes table

5. Who visited each store the most, and who visited the most stores?
   a. Chart Name: "Top Customers by Restaurants Visited"
      - Type: Table
      - Data source: customer_stats table
      - Columns: first_name, restaurants_visited, most_visited_restaurant
   
   b. Chart Name: "Most Frequent Visitors by Restaurant"
      - Type: Table
      - Data source: frequent_visitors table
      - Columns: restaurant_names, most_frequent_visitor, visit_count

## How to Run the Project

1. Ensure all dependencies are installed:
   ```
   poetry install
   ```

2. Run the DuckDB transformation script:
   ```
   poetry run python src/duckdb_transformation.py
   ```
   This will create the transformed CSV files in the `src/tmp/` directory.

3. Run the BigQuery setup script:
   ```
   poetry run python src/bigquery_setup.py
   ```
   This will load the data from `src/tmp/` into BigQuery.

4. Open Looker Studio and connect to your BigQuery dataset.

5. Create the visualizations as described in the "Visualizations" section.

6. To refresh data:
   - Re-run the DuckDB transformation script
   - Update BigQuery tables by running the BigQuery setup script
   - In Looker Studio, go to Resource > Manage added data sources and reconnect each data source

## Additional Considerations

### How would you build this differently if the data was being streamed from Kafka?

If the data was being streamed from Kafka, the architecture would need to be adapted for real-time processing:

1. **Data Ingestion**: 
   - Implement a Kafka consumer to continuously read data from relevant Kafka topics.
   - Consider using Kafka Connect for seamless integration with various data sources.
   - If the data is coming from a database, use Debezium for Change Data Capture (CDC) to stream changes directly into Kafka.

2. **Stream Processing**:
   - Replace batch processing with stream processing using tools like Apache Flink or Apache Spark Streaming.
   - Implement windowing techniques for aggregations (e.g., sliding windows for popular dishes in the last hour).

3. **Real-time Data Warehouse**:
   - Instead of BigQuery, consider using a database optimized for real-time analytics like ClickHouse, ksqlDB, or Databricks.
   - Alternatively, use BigQuery's streaming insert API for near-real-time data availability.

4. **Monitoring and Alerting**:
   - Implement robust monitoring for the streaming pipeline using tools like Prometheus and Grafana.
   - Set up alerting for data quality issues or processing delays.

5. **Scalability**:
   - Design the system to horizontally scale, allowing for increased data volume and velocity.
   - Use Kafka's partitioning to parallelize processing.

6. **Data Quality and Error Handling**:
   - Implement a Dead Letter Queue (DLQ) for messages that fail processing.
   - Add real-time data quality checks and anomaly detection.

7. **Visualization**:
   - Use real-time dashboarding tools that support streaming data sources, such as Grafana or Apache Superset.
   - Implement a caching layer to reduce load on the streaming system for frequently accessed metrics.

### How would you improve the deployment of this system?

To improve the deployment of this system, consider the following enhancements:

1. **Containerization**:
   - Dockerize the application components for consistency across environments.
   - Use Docker Compose for local development and testing.

2. **Infrastructure as Code (IaC)**:
   - Implement Terraform or CloudFormation to define and manage cloud resources.
   - Version control your infrastructure definitions.

3. **CI/CD Pipeline**:
   - Set up a robust CI/CD pipeline using tools like Jenkins or GitHub Actions.
   - Implement automated testing, including unit tests, integration tests, and end-to-end tests.

4. **Environment Management**:
   - Create separate environments for development, staging, and production.
   - Use environment-specific configuration management.

5. **Secrets Management**:
   - Utilize a secrets management tool like AWS Secrets Manager.
   - Avoid hardcoding sensitive information in the codebase or configuration files.

6. **Monitoring and Logging**:
   - Implement centralized logging using the ELK stack (Elasticsearch, Logstash, Kibana) or cloud-native solutions.

7. **Scalability and High Availability**:
   - Deploy the application across multiple availability zones.
   - Implement auto-scaling for components that may experience variable load.
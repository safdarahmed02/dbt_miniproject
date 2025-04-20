# Real-Time vs. Batch Analytics Pipeline

A small-scale data platform that ingests live data, processes it in real-time using Apache Spark Structured Streaming, stores both raw and aggregated results in MySQL, executes the same analytics in batch mode, and compares the accuracy and performance between the two modes.

## Project Overview

This project demonstrates how streaming and batch architectures coexist in modern data platforms, highlighting the trade-offs in latency, throughput, and eventual consistency. The system processes tweet-like data with hashtags, computes statistics over 20-minute windows, and provides detailed comparisons between real-time and batch processing approaches.

### Key Features

- Kafka-based data ingestion at 100+ messages per second
- Real-time analytics using Spark Structured Streaming with 20-minute tumbling windows
- Batch processing that mirrors the streaming logic
- Performance and accuracy comparison dashboard
- Containerized deployment with Docker Compose

## System Architecture

```
[Data Source] ─▶ Producer.py ─▶ Kafka (raw_feed) 
                                     │
       ┌─────────────────────────────┘
       ▼
Spark Structured Streaming
- Deserialize & clean
- Window (20 min tumbling)
- Aggregations (count, topN, avg)
- Write to Kafka (proc_feed)
- Write to MySQL (jdbc_results)
       │
       ▼
     MySQL
       │
       │ Nightly @23:55
       ▼
Batch Job (Spark SQL)
- Read full table
- Run same aggregations
- Store batch_results
       │
       ▼
Comparison Script
- Join stream_results vs batch_results
- Compute deltas, latency
- Export CSV + charts
```

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- 4+ CPU cores and 8GB+ RAM recommended

## Installation & Setup

1. Clone this repository:
```bash
git clone <repository-url>
cd dbt_pipeline
```

2. Start the infrastructure using Docker Compose:
```bash
docker-compose up -d
```

3. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Running the Pipeline

### Starting All Services

Run the entire pipeline (producer, streaming job, and batch scheduler):

```bash
python main.py start
```

### Running Individual Components

Start only the tweet producer:
```bash
python main.py producer
```

Start only the Spark Streaming job:
```bash
python main.py streaming
```

Run the batch processing job once:
```bash
python main.py batch
```

Start the batch scheduler (runs at 23:55 IST daily):
```bash
python main.py scheduler
```

Run the comparison analysis:
```bash
python main.py compare
```

## Data Flow

1. **Producer**: Generates fake tweets with hashtags and sends them to Kafka `raw_feed` topic at 100+ messages/second.

2. **Streaming Job**: 
   - Reads from Kafka `raw_feed`
   - Processes data in 20-minute tumbling windows
   - Computes hashtag frequencies and statistics
   - Writes results to MySQL `stream_window_stats` table
   - Sends processed results to Kafka `results_feed` topic

3. **Batch Job**:
   - Runs nightly at 23:55 IST
   - Reads all data from MySQL `tweet_raw` table
   - Performs the same aggregations as the streaming job
   - Writes results to MySQL `batch_window_stats` table

4. **Comparison Analysis**:
   - Compares streaming vs batch results
   - Calculates accuracy metrics (% difference)
   - Measures latency and throughput
   - Generates CSV reports and visualization charts

## Performance Metrics

The system is designed to meet the following performance targets:

- **Functional Parity**: Streaming and batch outputs match within 1% error margin
- **Latency**: < 5 seconds end-to-end for a 20-minute window
- **Throughput**: ≥ 100 records per second on a four-core laptop

## Project Structure

```
dbt_pipeline/
├── docker-compose.yml           # Docker services configuration
├── requirements.txt             # Python dependencies
├── main.py                      # Main entry point
├── producer/                    # Data producer component
│   └── tweet_producer.py        # Generates and sends data to Kafka
├── streaming/                   # Streaming processing component
│   └── spark_streaming.py       # Spark Structured Streaming job
├── batch/                       # Batch processing component
│   ├── batch_processor.py       # Batch processing job
│   └── scheduler.py             # Scheduler for batch job
├── comparison/                  # Comparison and analysis component
│   ├── comparison_analyzer.py   # Compares streaming vs batch results
│   └── output/                  # Generated reports and charts
└── scripts/                     # Helper scripts
    └── init.sql                 # MySQL initialization script
```

## Troubleshooting

- **Kafka Connection Issues**: Ensure Kafka is running with `docker-compose ps`. Check if topics are created with `kafka-topics.sh --list --bootstrap-server localhost:9092`.

- **Spark Job Failures**: Check Spark logs in the container with `docker logs spark-master`. Ensure MySQL is accessible from Spark.

- **MySQL Connection Issues**: Verify MySQL is running with `docker-compose ps`. Check credentials in configuration files.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributors

- Your Name - [Your GitHub](https://github.com/yourusername)
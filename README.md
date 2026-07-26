# 🚀 Real-Time CDC Pipeline  
## PostgreSQL → Debezium → Kafka → Spark Structured Streaming → Delta Lake / Parquet

![Architecture](docs/images/cdc_architecture.png)

## 📌 Project Overview

This project demonstrates a real-time Change Data Capture (CDC) pipeline built with modern Data Engineering technologies.

The goal of this project is to capture database changes from PostgreSQL (`INSERT`, `UPDATE`, `DELETE`) in real time and process them through a streaming architecture.

The pipeline simulates a production-style data platform:

```
PostgreSQL Database
        |
        |
        v
Debezium CDC Connector
        |
        |
        v
Apache Kafka
        |
        |
        v
Spark Structured Streaming
        |
        |
        +----------------+
        |                |
        v                v
 Delta Lake          Parquet Files
(Bronze Layer)       (Data Storage)
```

---

# 🏗 Architecture

## Data Flow

### 1. PostgreSQL (Source Database)

PostgreSQL acts as the transactional source system.

The database generates changes:

- INSERT
- UPDATE
- DELETE

Example:

```sql
INSERT INTO person
VALUES (1,'Giorgi','Georgia');
```

The change is written into PostgreSQL WAL (Write Ahead Log).

---

### 2. Debezium CDC Connector

Debezium captures database changes from PostgreSQL logical replication.

It converts database operations into CDC events:

Example event:

```json
{
  "op":"c",
  "before":null,
  "after":{
      "id":1,
      "name":"Giorgi"
  }
}
```

Operation types:

| Operation | Meaning |
|---|---|
| c | CREATE / INSERT |
| u | UPDATE |
| d | DELETE |
| r | READ / Snapshot |

---

### 3. Apache Kafka

Kafka works as a distributed event streaming platform.

Debezium publishes events into Kafka topics.

Example:

```
postgres.public.person
postgres.public.weather_data
```

Kafka provides:

- Message durability
- Replay capability
- Scalability
- Decoupling between systems

---

### 4. Spark Structured Streaming

Spark consumes CDC events from Kafka.

Responsibilities:

- Parse JSON events
- Apply transformations
- Handle INSERT / UPDATE / DELETE
- Write streaming results

Example:

```
Kafka Topic
      |
      |
      v
Spark Streaming
      |
      |
      v
Delta Lake Bronze Table
```

---

### 5. Delta Lake / Parquet Storage

Processed data is stored in analytical formats.

### Delta Lake

Provides:

- ACID transactions
- Schema evolution
- Time travel
- MERGE operations


### Parquet

Columnar storage format optimized for analytics.

Example:

```
data/
 |
 +-- bronze/
 |
 +-- silver/
 |
 +-- parquet/
```

---

# 🛠 Technologies

| Technology | Purpose |
|-|-|
| PostgreSQL | Source OLTP database |
| Debezium | CDC data capture |
| Apache Kafka | Event streaming platform |
| Kafka Connect | Connector framework |
| Spark Structured Streaming | Stream processing |
| Delta Lake | Lakehouse storage |
| Apache Parquet | Columnar storage |
| Docker Compose | Local infrastructure |

---

# 📂 Project Structure

```
CDC_project/

│
├── docker-compose.yml
│
├── postgres/
│   └── init.sql
│
├── debezium/
│   └── connector.json
│
├── kafka/
│
├── spark/
│   └── sparkStream.py
│
├── delta/
│
├── parquet/
│
└── README.md
```

---

# 🐳 Running the Project

## 1. Start Infrastructure

```bash
docker compose up -d
```

Check containers:

```bash
docker ps
```

Expected services:

```
postgres
kafka
debezium-connect
spark
kafka-ui
```

---

# 2. Create Debezium Connector

Example:

```bash
curl -X POST \
http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @connector.json
```

Check status:

```bash
curl http://localhost:8083/connectors/postgres-connector/status
```

Expected:

```
RUNNING
```

---

# 3. Generate Database Changes

Example:

## INSERT

```sql
INSERT INTO person
(id,name,email)
VALUES
(1,'Giorgi','test@gmail.com');
```


## UPDATE

```sql
UPDATE person
SET name='George'
WHERE id=1;
```


## DELETE

```sql
DELETE FROM person
WHERE id=1;
```

---

# 4. Start Spark Streaming

Run:

```bash
spark-submit sparkStream.py
```

Spark consumes Kafka CDC events:

```
Kafka Topic
       |
       |
       v
Spark Structured Streaming
       |
       |
       v
Delta Lake
```

---

# 📊 Example CDC Event

INSERT:

```json
{
"op":"c",
"after":{
"id":1,
"name":"Giorgi"
}
}
```

UPDATE:

```json
{
"op":"u",
"before":{
"name":"Giorgi"
},
"after":{
"name":"George"
}
}
```

DELETE:

```json
{
"op":"d",
"before":{
"id":1
}
}
```

---

# 🔥 Key Data Engineering Concepts Demonstrated

✅ Change Data Capture (CDC)

✅ Event Driven Architecture

✅ Real-Time Streaming

✅ Kafka Message Processing

✅ Spark Structured Streaming

✅ Lakehouse Architecture

✅ Delta Lake Transactions

✅ Dockerized Data Platform


---

# 🚀 Future Improvements

Production improvements:

- Add Apache Airflow orchestration
- Add Data Quality checks
- Add Monitoring with Prometheus/Grafana
- Deploy on Kubernetes
- Add Schema Registry
- Implement Medallion Architecture:
    - Bronze
    - Silver
    - Gold

---

# 👨‍💻 Author

**Giorgi Megeneishvili**

Data Engineer | Data Developer

GitHub:
https://github.com/GiorgiMegeneishvili

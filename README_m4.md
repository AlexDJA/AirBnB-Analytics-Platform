---

# Milestone 4 — AWS Deployment

**Submitted by:** Alexandre DJADJAGLO — Student ID 40243644

This milestone deploys the full M3 system on AWS infrastructure. Local
HDFS is retired and replaced by S3 for storage; the rest of the stack
(MongoDB, ElasticSearch, ingest, Spark, agent) runs on a single EC2
instance via Docker Compose. The agent is reachable publicly over HTTP.

## 19. M4 architecture

```
                    ┌─────────────────────────────────────┐
                    │   AWS Cloud — us-east-2 (Ohio)      │
                    │                                     │
   ┌────────┐       │  ┌───────────────────────────────┐  │
   │  User  │──────▶│  │  EC2 c7i-flex.large           │  │
   │ browser│ :8000 │  │  IAM Role: S3 read/write       │  │
   └────────┘       │  │                               │  │
                    │  │  Docker Compose stack:        │  │
                    │  │   • mongodb                   │  │
                    │  │   • elasticsearch             │  │
                    │  │   • ingest      (boto3)       │  │
                    │  │   • spark_pipeline (s3a)      │  │
                    │  │   • agent  (FastAPI + Vue)    │  │
                    │  └─────────────┬─────────────────┘  │
                    │                │                    │
                    │       reads ▼  ▼ writes             │
                    │  ┌─────────────────────────────┐    │
                    │  │ S3: airbnb-m4-alexdja       │    │
                    │  │   • data/        (raw CSV)  │    │
                    │  │   • processed/   (Parquet)  │    │
                    │  └─────────────────────────────┘    │
                    └─────────────────────────────────────┘
                                  │
                                  │ (LLM calls)
                                  ▼
                          ┌─────────────────┐
                          │  OpenRouter API │
                          └─────────────────┘
```

Key changes vs M3:
- **HDFS removed entirely** — no more `namenode` / `datanode` services
  in the compose. The raw CSV and the processed Parquet both live on S3.
- `ingest.py` reads from S3 (boto3) and writes to S3 (boto3).
- `spark_pipeline.py` reads from S3 via the `s3a://` protocol using
  the Hadoop S3A connector plus the AWS SDK bundle.
- The EC2 instance has an IAM Role (`airbnb-m4-ec2-role`) attached so
  no AWS access keys are stored on the box. boto3 and the S3A connector
  both pick up credentials from the EC2 instance metadata endpoint.

## 20. Running the AWS deployment

### Prerequisites

- AWS account with permission to create S3, EC2, IAM resources
- The CSV `AirbnbEuropeMarket.csv` already uploaded to
  `s3://<bucket>/data/`
- An IAM Role with `AmazonS3FullAccess` attached to the target EC2
  instance
- A security group on the EC2 allowing inbound TCP on port 8000 from
  `0.0.0.0/0` and SSH (22) from your IP

### Deployment steps

```bash
# On the EC2 host (Amazon Linux 2023)
sudo dnf update -y
sudo dnf install -y docker git
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user

# Install Docker Compose v2 plugin
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose

# Log out + back in for the docker group to take effect
exit
ssh -i <key>.pem ec2-user@<public-ip>

# Clone and configure
git clone https://github.com/<user>/project-airbnb.git
cd project-airbnb
cat > .env <<'EOF'
MONGO_INITDB_ROOT_USERNAME=admin
MONGO_INITDB_ROOT_PASSWORD=lab1pass
MONGO_INITDB_DATABASE=cebd1261
MONGODB_URI=mongodb://admin:lab1pass@mongodb:27017/cebd1261?authSource=admin
ES_URL=http://elasticsearch:9200
OPENROUTER_API_KEY=<your-key>
OPENROUTER_MODEL=mistralai/ministral-14b-2512
AWS_REGION=us-east-2
S3_BUCKET=<your-bucket>
S3_CSV_KEY=data/AirbnbEuropeMarket.csv
S3_PARQUET_KEY=processed/airbnb_europe.parquet
EOF

# Bring everything up
docker compose up -d
docker compose run --rm spark_pipeline   # populate Mongo + ES from S3
docker compose restart agent
```

Open `http://<public-ip>:8000` in your browser — the agent should
respond to natural-language questions about the dataset.

### Example demo questions

These three questions are declared for the live demo:

1. **(Summary)** *How many listings are in the dataset and what's
   the average rating?*
2. **(Top-N)** *What are the top 5 cities by total revenue?*
3. **(Anomaly)** *Show me listings with revenue per booked day above
   $2000?*

Real outputs from the AWS deployment:

```
Q1 → "There are 95,412 total listings in the database. The average
      rating across all listings is 4.77 out of 5."
Q2 → "1. City of Edinburgh — £20.2M  2. Palma — £16.8M  3. City of
      Westminster — £16.2M  4. Barcelona — £16.0M  5. Cotswold
      District — £13.8M"
Q3 → "10 outlier listings, top earner: Derbyshire Dales (UK)
      $4582.75/day on 20 booked days."
```

## 21. Known limitations

- **Single-instance deployment.** No high availability — if the EC2 box
  dies, the agent is down. A future iteration would put MongoDB and
  ElasticSearch on managed services (DocumentDB / OpenSearch) and run
  the stateless agent on ECS or Lambda.
- **Fallback ES translation is partial.** Only `$match` and `$limit`
  pipeline stages are translated to ES queries. `$group`, `$facet`,
  `$bucket` are not supported in fallback mode — those questions will
  return "No data found" if Mongo is down.
- **OpenRouter free-tier rate limits** can occasionally cause the
  pipeline-generation call to fail. The agent returns a clean error
  message in that case rather than crashing.
- **No persistent volumes on EC2.** If the instance is terminated,
  MongoDB and ElasticSearch state is lost — but the data is rebuilt
  from S3 in <2 minutes by re-running the Spark job.

## 22. M4 deliverables checklist

- [x] EC2 instance running with Docker Compose stack (5 services)
- [x] S3 bucket with raw CSV (data/) and processed Parquet (processed/)
- [x] Public URL: http://<ec2-public-ip>:8000
- [x] IAM Role attached to EC2 (no credentials on disk)
- [x] All M3 safeguards still functional on AWS
- [x] 3 demo questions declared
- [x] Architecture diagram showing 7 layers and AWS services
- [x] Updated REFLECTION.md
- [X] GitHub release tagged `M4`
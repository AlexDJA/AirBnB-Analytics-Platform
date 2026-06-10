# REFLECTION — Milestone 4

## What I built for M4

- Migrated the full M3 system to AWS. The dataset (raw CSV + processed Parquet) now lives on **S3**; the application stack (`mongodb`, `elasticsearch`, `ingest`, `spark_pipeline`, `agent`) runs on a single **EC2 c7i-flex.large** instance via Docker Compose; the agent is reachable publicly on port 8000.
- **Removed HDFS entirely** from the project. The `namenode` and `datanode` services are gone from `docker-compose.yml`, and the `hadoop/` config directory is no longer referenced. The previous M1/M2 pipeline read a local Parquet from HDFS; the M4 version reads it from S3 via the `s3a://` protocol.
- Rewrote `ingest.py` to use **boto3** for both the CSV read and the Parquet write. The same code works on my laptop (via `.env` access keys) and on EC2 (via the attached IAM Role) without modification.
- Reconfigured Spark to talk to S3: added `org.apache.hadoop:hadoop-aws:3.3.4` + `com.amazonaws:aws-java-sdk-bundle:1.12.367` to the `--packages`, switched the credentials provider to `com.amazonaws.auth.DefaultAWSCredentialsProviderChain`, and pointed the S3A endpoint at `s3.us-east-2.amazonaws.com`.
- Set up the AWS side from scratch: IAM user for local dev, IAM Role `airbnb-m4-ec2-role` with `AmazonS3FullAccess` attached to the EC2 instance, security group `airbnb-m4-sg` opening only ports 22 (SSH) and 8000 (agent), key pair `airbnb-m4-key.pem` for SSH access.

## The hardest problem I solved

- **`NoAwsCredentialsException` on Spark when reading S3 from EC2.** The first time I ran `spark_pipeline` on the deployed instance, Spark crashed with `org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException: SimpleAWSCredentialsProvider: No AWS credentials in the Hadoop configuration` — even though `boto3` (used by `ingest.py`) worked fine on the same machine.
- The root cause: I had hardcoded `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` in the Spark session config. That provider only looks at environment variables — and on EC2 with an IAM Role, there are no `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars to find. The role's credentials live behind the EC2 instance metadata endpoint (`169.254.169.254`), which `SimpleAWSCredentialsProvider` doesn't know about.
- **The fix** was a one-line change: replace `SimpleAWSCredentialsProvider` with `com.amazonaws.auth.DefaultAWSCredentialsProviderChain`. This is a composite provider that tries, in order: env vars → Java system properties → `~/.aws/credentials` → EC2 instance metadata → ECS task role. On my laptop it finds the env vars from `.env`; on EC2 it finds the IAM Role's temporary credentials in the metadata. The same code, the same JAR, two environments. Zero `if cloud:` branches.
- **The bigger lesson**: when a piece of code uses external auth, prefer the most general credential resolution chain available. The cost of generality is ~zero (it just tries more providers); the benefit is that the same artifact runs in dev and prod.

## One design decision I made

- The decision was around the storage layer for M4: keep HDFS (running namenode + datanode on the EC2) and use S3 only as a backup, or replace HDFS entirely with S3?
- I went with **replacing HDFS entirely**. Both the raw CSV and the processed Parquet live on S3; Spark reads from S3 via `s3a://` and writes to MongoDB + ElasticSearch on the same EC2.
- The trade-off:
  - **Latency** — S3 reads are ~50ms vs ~10ms for HDFS local. For my 95k-row dataset and a single Spark job, this is invisible.
  - **Durability** — S3 has 11 nines of durability by design. A single-instance HDFS deployment has effectively zero replication (replication factor 3 on one datanode is meaningless), so I'd have *worse* durability than S3 with much more operational complexity.
  - **RAM** — namenode + datanode together would have consumed ~1.5 GB of RAM that I'd rather give to ElasticSearch (which is RAM-hungry by nature) and Spark (driver memory).
  - **Lifecycle decoupling** — with S3, I can `terminate` the EC2 instance and recreate it later without losing the data. Re-running Spark rebuilds Mongo + ES from S3 in ~2 minutes. With HDFS-on-EC2, terminating the instance would erase the dataset.
- The brief explicitly states *"Store dataset on AWS S3 — replace local HDFS storage for cloud deployment"*. My interpretation was the most literal possible: **remove HDFS, period**. Looking back, this was the right call — running a distributed file system on a single node defeats its purpose, and the brief is asking us to learn the cloud-native pattern of storage/compute separation.

## AI tools used

- Claude AI has been the only AI tool used for this Milestone 4.
- Generate the boto3 skeleton for the new `ingest.py`, explain the difference between `SimpleAWSCredentialsProvider` and `DefaultAWSCredentialsProviderChain`, help me debug the `NoAwsCredentialsException` error, and suggest the EC2 instance type based on my memory budget. Also helped me write the README_M4.md.
- Throughout the deployment, I verified each step with a concrete test (curl `/healthz` after each restart, IAM Role verification via `curl http://169.254.169.254/latest/meta-data/iam/security-credentials/`, presence of Parquet in S3 after ingest, etc.). The "AI suggestion + immediate test" combination is faster and safer than doing everything at once.

## What I would improve with more time

- Try **Microsoft Azure** hosting system for more reliability and consistency during prime time usage — would allow me to . 
- **CI/CD pipeline.** Right now I deploy by SSH-ing into the box and running `git pull && docker compose up -d`. A proper setup would push a built Docker image to **ECR**, and a **GitHub Actions** workflow would `docker compose pull && up -d` on the box (or update an ECS service definition). One push, one deploy.
- **Observability.** I'd wire **CloudWatch Logs** for the container logs and **CloudWatch Metrics + Alarms** for "agent error rate", "Mongo connection failures", "S3 read latency". For LLM-specific observability I'd add **Langfuse** (mentioned in M3 reflection) — at this point I've earned the right to see exactly what every prompt/response pair looks like over time.
# 🚀 Multi-Server CI/CD Deployment Guide (GitHub Actions)

This pipeline automatically deploys `log-agent` and `node-exporter` across all your EC2 instances whenever you push code or trigger a deployment in GitHub Actions.

---

## 🔑 1. GitHub Repository Secrets Setup

In your GitHub repository:  
**Settings** ➔ **Secrets and variables** ➔ **Actions** ➔ **New repository secret**

Set these secrets:

| Secret Name | Supported Fallback Aliases | Description / Value | Example |
| :--- | :--- | :--- | :--- |
| **`EC2_USER`** | `HOST_USER`, `SSH_USER` | SSH Username on your EC2 instances *(default: `ubuntu`)* | `ubuntu` (or `ec2-user`) |
| **`EC2_HOSTS`** | `SERVER_HOSTS`, `HOSTS` | List of EC2 Public DNS hostnames or IP addresses (comma, space, or newline separated) | `ec2-xx-xx-xx-xx.compute-1.amazonaws.com, ec2-yy-yy-yy-yy.compute-1.amazonaws.com` |
| **`SSH_PRIVATE_KEY`** | `EC2_SSH_KEY` | Your EC2 SSH Private Key (`.pem` file content) | `-----BEGIN OPENSSH PRIVATE KEY-----...` |
| **`ENV_FILE`** | — | Complete `.env` file content | *(Paste your entire `.env` file)* |
| **`DEPLOY_PATH`** *(Optional)* | — | Directory path on remote servers | `/home/ubuntu/ec2-setup` (default) |

---

### 📝 How to Format `EC2_HOSTS`:
You can paste multiple EC2 Public DNS hostnames or IPs either comma-separated or one per line:

```text
ec2-xx-xx-xx-xx.compute-1.amazonaws.com, ec2-yy-yy-yy-yy.compute-1.amazonaws.com
```

---

### 📝 Example `ENV_FILE` (1MB Memory Buffering):
Directly copy and paste your complete `.env` file into this secret:

```env
S3_BUCKET=your-log-bucket-name
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
S3_PREFIX=logs/
BUFFER_SIZE_MB=1
FLUSH_INTERVAL=30m
PORT=8081

# Array of monitored machines for dashboard dropdown
SERVERS=[{"id":"local","name":"Localhost Dev","ip":"127.0.0.1","metrics_url":"http://localhost:8080","agent_url":"http://localhost:8081"},{"id":"ec2-prod","name":"EC2 Production","ip":"your-server-ip","metrics_url":"http://your-server-ip:8080","agent_url":"http://your-server-ip:8081"}]
```

---

## ⚡ 2. 1MB In-Memory Buffering Architecture

- **Eliminates the "Small File Problem"**: Rather than creating thousands of tiny 2KB files in S3, logs accumulate in memory until reaching **1MB**.
- **Optimized Parquet Files**: Once 1MB of logs is collected for a container, it compresses the batch with Snappy and pushes a single, high-efficiency Parquet file to S3.
- **Safety Max Interval (`FLUSH_INTERVAL=30m`)**: If a container produces very low log volume and doesn't hit 1MB within 30 minutes, it automatically flushes so no logs are held indefinitely.
- **Zero Data Loss on Shutdown**: On application stop or container restart, all buffered records are flushed immediately.

---

## ⚙️ 3. Remote EC2 Prerequisites (First-Time Only)

On each EC2 instance:
```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin rsync
sudo usermod -aG docker $USER
```
*(Make sure ports `8081` and `9100` are allowed in the EC2 Security Group)*.

---

## 🎯 4. Deployment Flow

- **Trigger**: Automatic on `git push` to `main`/`master`, or manual via **Actions ➔ Run workflow**.
- **Execution**:
  1. Tests code and checks compose config.
  2. For each host in `EC2_HOSTS`:
     - Gracefully stops previous running versions.
     - Syncs repository code via rsync.
     - Writes `ENV_FILE` directly to `.env` using lossless Base64 transfer.
     - Builds and launches containers with `docker compose up -d --build --force-recreate`.
     - Validates `/health` endpoint with retry checks.
     - Runs `docker system prune -f` at the end to clean up and free disk space.

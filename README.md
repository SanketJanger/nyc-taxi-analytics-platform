# 🚕 NYC Yellow Taxi Analytics Platform

A full-stack data lakehouse and conversational analytics platform built on top of 38M+ NYC Yellow Taxi trip records, combining AWS cloud infrastructure with an AI-powered natural language query interface.

---

## 🏗️ Architecture Overview

```
S3 (Parquet) → Athena (SQL) → FastAPI Backend → Next.js Frontend
                                                       ↑
                                            Vercel AI SDK (NL → SQL)
```

---

## ✨ Features

- **Data Lakehouse** — Queries 38M+ taxi trip records stored on AWS S3 in Parquet format with column pruning and partition-based query cost optimization via Amazon Athena
- **RESTful API** — FastAPI backend exposing 15+ predefined analytics endpoints covering trip trends, fare distributions, borough breakdowns, and more
- **Conversational AI Layer** — Natural language to optimized SQL translation using the Vercel AI SDK, with real-time streaming responses
- **Interactive Frontend** — Next.js UI with live query streaming and dynamic analytics views
- **Automated CI/CD** — GitHub Actions pipeline for continuous integration and deployment

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Storage | AWS S3 (Parquet format) |
| Query Engine | Amazon Athena |
| Backend | FastAPI (Python) |
| Frontend | Next.js |
| AI Layer | Vercel AI SDK |
| CI/CD | GitHub Actions |

---

## 📊 Key Metrics

- **38M+** taxi trip records queried
- **15+** predefined analytics views
- Parquet partitioning + column pruning for optimized query cost
- Real-time streaming responses via Next.js frontend

---

## 🚀 Getting Started

### Prerequisites

- Python 3.10+
- Node.js 18+
- AWS account with S3 and Athena access configured

### Backend Setup

```bash
# Clone the repository
git clone https://github.com/SanketJanger/nyc-taxi-analytics.git
cd nyc-taxi-analytics

# Install Python dependencies
pip install -r requirements.txt

# Configure AWS credentials
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=us-east-1

# Run FastAPI server
uvicorn main:app --reload
```

### Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

---

## 📁 Project Structure

```
├── backend/
│   ├── main.py          # FastAPI entry point
│   ├── routes/          # API route handlers
│   ├── athena/          # Athena query utilities
│   └── ai/              # Vercel AI SDK integration
├── frontend/
│   ├── app/             # Next.js app directory
│   └── components/      # UI components
├── .github/
│   └── workflows/       # CI/CD pipelines
└── README.md
```

---

## 📬 Author

**Sanket Janger**
[LinkedIn](https://linkedin.com/in/sanketjanger) • [GitHub](https://github.com/SanketJanger) • [Portfolio](https://sanketjanger.dev)

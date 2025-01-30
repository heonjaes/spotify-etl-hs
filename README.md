# Spotify ETL Pipeline

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Spotify](https://img.shields.io/badge/Spotify-1ED760?&style=for-the-badge&logo=spotify&logoColor=white)
## Overview
`spotify-etl-hs` is an ETL (Extract, Transform, Load) pipeline that extracts a user's listening history from the Spotify API, transforms the data for analysis, and loads it into a PostgreSQL database. The pipeline is orchestrated using Apache Airflow and leverages Spark for data transformation.

## Tech Stack
- **Spotify API** – Extracts user listening history
- **Python 3.12** – Handles API requests and data processing
- **Apache Airflow** – Orchestrates and schedules the ETL workflow
- **Apache Spark** – Performs scalable data transformations
- **PostgreSQL** – Stores the processed data
- **Docker** – Containerizes the ETL pipeline for portability

## Features
- **Automated Data Extraction**: Pulls Spotify listening history via API requests
- **Scalable Data Transformation**: Uses Spark to process and clean data efficiently
- **Database Integration**: Loads structured data into PostgreSQL for further analysis
- **Workflow Automation**: Orchestrates the entire process with Apache Airflow
- **Containerized Deployment**: Uses Docker for easy setup and execution

## Project Structure
```
spotify-etl-hs/
│── dags/                   # Airflow DAGs for orchestration
│── scripts/                # Python scripts for API calls and transformations
│   ├── auth/               # Authentication scripts
│   ├── etl/                # ETL processing scripts
│   ├── database/           # Database interaction scripts
│── config/                 # Configuration files
│── logs/                   # Log files
│── docker-compose.yaml     # Docker Compose setup
│── Dockerfile              # Docker container definition
│── requirements.txt        # Dependencies
│── README.md               # Project documentation
```

## Setup Instructions
### Prerequisites
Ensure you have the following installed:
- Python 3.12
- Apache Airflow
- Apache Spark
- PostgreSQL
- Docker & Docker Compose
- Spotify Developer Account & API Credentials

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/spotify-etl-hs.git
   cd spotify-etl-hs
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up your **Spotify API credentials**:
   - Create an `.env` file in the root directory with:
     ```env
     SPOTIFY_CLIENT_ID=your_client_id
     SPOTIFY_CLIENT_SECRET=your_client_secret
     ```
4. Configure PostgreSQL:
   - Create a database and update connection details in `config/database_config.yaml`

5. Start Airflow:
   ```bash
   airflow db init
   airflow webserver & airflow scheduler
   ```
6. Deploy DAGs by placing them in the `dags/` directory and triggering via the Airflow UI.

7. Run the ETL pipeline with Docker:
   ```bash
   docker-compose up
   ```

## License
This project is licensed under the MIT License.

---
Contributions are welcome! Feel free to open issues and pull requests.


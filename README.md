# E-commerce Analytics Pipeline | An ELT Project

## Project Overview

This project implements a complete ELT (Extract, Load, Transform) pipeline for the public Brazilian e-commerce dataset from Olist. The primary goal is to ingest raw multi-file CSV data, load it into a cloud-based PostgreSQL database, and transform it into a clean, structured analytical data mart. This data mart serves as a single source of truth for business intelligence and is visualized through an interactive Power BI dashboard to answer key business questions.

This project was developed as a practical exercise in data engineering fundamentals.

## 🚀 Final Dashboard

The final dashboard provides insights into sales trends, customer geography, and product category performance.

* **An interactive version is available here:** (https://app.powerbi.com/groups/me/reports/58eea2b5-53ee-4497-966b-48f6b31d80de/5da39f28c0041b893b08?experience=power-bi)

* **[Download the full PDF Report](assets/sales_analysis_in_Brazil.pdf)**
![E-commerce Analytics Dashboard Preview](assets/dashboard_preview.png)

## 🛠️ Tech Stack

* **Language:** Python 3.11
* **Libraries:** Pandas, SQLAlchemy, psycopg2, python-dotenv
* **Database:** PostgreSQL (Cloud-hosted on Neon)
* **BI Tool:** Power BI
* **Containerization:** Docker & Docker Compose

## 🏛️ Architecture

The project follows the ELT (Extract, Load, Transform) principle:

1.  **Extract:** A Python script (`load_raw_data.py`) reads multiple raw CSV files from the `/data` directory using Pandas.
2.  **Load:** The same script performs essential technical cleaning (corrects data types, standardizes column names) and loads the dataframes into a `raw_data` schema in the cloud PostgreSQL database.
3.  **Transform:** A separate Python script (`transform_data.py`) executes a SQL script (`build_analytics_mart.sql`) against the database. This script transforms the raw data into a clean, relational star schema within a new `analytics` schema. This creates the final dimension tables (`dim_customers`, `dim_products`) and a central fact table (`fact_orders`).

The entire process is designed to be run within a containerized environment using Docker Compose to ensure consistency and reproducibility.


## ⚙️ How to Run the Project

This project is fully containerized and the entire ELT process is managed by Docker Compose.

### Prerequisites
* Docker & Docker Compose installed.
* Git.
* A cloud PostgreSQL database (e.g., from Neon).

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/0marsm0/Analytical_Data_Mart_for_E-commerce.git (https://github.com/0marsm0/Analytical_Data_Mart_for_E-commerce.git)
    cd e-commerce-analytics
    ```

2.  **Create the environment file:**
    Create a `.env` file in the project root and populate it with your PostgreSQL database credentials:
    ```env
    POSTGRES_USER=your_db_user
    POSTGRES_PASSWORD=your_db_password
    POSTGRES_HOST=your_db_host
    POSTGRES_DB=your_db_name
    ```

3.  **Place the data:**
    Download the Olist dataset from Kaggle and place the required CSV files inside the `/data` directory.

### Running the Pipeline

The pipeline consists of two main steps that need to be run in sequence.

1.  **Run the Extract & Load Step:**
    This command builds the Docker image and runs the first script, which loads the raw CSV data into the `raw_data` schema in your database.
    ```bash
    docker-compose up --build
    ```

2.  **Run the Transform Step:**
    After the first step is complete, run this command. It executes the second script, which runs the SQL transformations to build the `analytics` data mart.
    ```bash
    docker-compose run --rm app python transform_data.py
    ```

### Viewing the Results

Once the pipeline has finished, you can connect Power BI or any other BI tool to your cloud PostgreSQL database using the credentials from your `.env` file. The final, cleaned tables will be available in the `analytics` schema.
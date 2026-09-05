# E-Commerce Data Engineering Pipeline

An end-to-end **Data Engineering project** that builds an automated e-commerce data pipeline using Python, PostgreSQL, Docker, Apache Airflow, and Power BI.

The project focuses on data ingestion, validation, transformation, analytics, data quality checks, and business reporting.


## Architecture

                    CSV DATA
                       │
                       ▼
                ┌──────────────┐
                │   Airflow    │
                │  Orchestration│
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐
                │   PostgreSQL │
                │   Raw Data   │
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐
                │ Data Quality │
                │   Checks     │
                └──────┬───────┘
                       │
                       ▼
                     Valid  
                       │      
                       ▼      
                   Transform  
                       │
                       ▼
                ┌──────────────┐
                │   Analytics  │
                │    Tables    │
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐
                │   Power BI   │
                │  Dashboard   │
                └──────────────┘

## Project Goals

The main goal of this project is to simulate a real-world data engineering workflow where raw e-commerce data is:

* loaded into a PostgreSQL database
* validated through automated data quality checks
* transformed into analytical datasets
* orchestrated with Apache Airflow
* stored in analytical tables
* visualized in Power BI

## Technologies Used

| Technology     | Purpose                                       |
| -------------- | --------------------------------------------- |
| Python         | Data ingestion, transformation and validation |
| PostgreSQL     | Relational database and analytical storage    |
| Apache Airflow | Pipeline orchestration                        |
| Docker         | Containerization and reproducible environment |
| Pandas         | Data processing                               |
| SQLAlchemy     | PostgreSQL connectivity                       |
| Power BI       | Data visualization and business analytics     |
| Git / GitHub   | Version control                               |


## Dataset

The project uses a free to use e-commerce dataset containing:
* Customers
* Orders
* Order Items
* Products
* Reviews

Example relationships:

```text
Customers
    │
    └──── Orders
              │
              └──── Order Items
                        │
                        └──── Products

Customers ───── Reviews ───── Products
```
## Pipeline

The main Airflow DAG follows this workflow:

```text
              load_data
                  │
                  ▼
              validate_data
                  │
                  ▼
            transform_data
            /      |      \
           ▼       ▼       ▼
daily_sales  category_sales  weekday_sales

```
### 1. Data Loading

CSV files are loaded into PostgreSQL using Python and Pandas.

The ingestion process loads:

-customers
-orders
-order_items
-products
-reviews

### 2. Data Quality

Before transformation, the pipeline performs automated validation checks.

Current checks include:

* Empty table detection
* NULL primary keys
* Duplicate primary keys
* NULL values in required columns
* Foreign key validation
* Positive quantities
* Non-negative prices
* `line_total` consistency with `quantity × unit_price`


## Transformations

The main analytical dataset is `fact_sales`.

It combines:

* orders
* order items
* products
* customers

and calculates business metrics such as:

-line_total
-profit
-quantity
-category
-city
-order_date

## Analytical Tables

The pipeline creates several analytical tables.

### `fact_sales`

Detailed sales-level dataset containing order, customer, product and profitability information.

### `daily_sales`

Daily aggregated metrics:

* Total Orders
* Total Items
* Total Sales
* Total Profit

### `category_sales`

Category-level metrics:

* Total Items
* Total Sales
* Total Profit

### `weekday_sales`

Sales performance by weekday:

* Total Orders
* Total Sales
* Total Profit

These tables are designed to simplify downstream reporting and analytics.

---

## Power BI

Power BI is connected directly to PostgreSQL and is used for business analysis.

The dashboard includes KPIs such as:

* Total Sales
* Total Profit
* Total Orders
* Total Items
* Profit Margin
* Average Order Value

It also includes analysis of:

* Sales over time
* Sales by category
* Profit by category
* Sales and profit by weekday
* Top products by sales
* Top products by profit
* Sales and profit by city
* Customer review metrics

## Docker

The project runs in a containerized environment using Docker Compose.

The environment contains the services required for the pipeline, including:

* PostgreSQL
* Apache Airflow
* Airflow scheduler
* Airflow webserver
* Airflow DAG processor

This makes the project reproducible across different environments.

## Project Structure

```text
Project-Ecommerce/
│
├── dags/
│   └── ecommerce_pipeline.py
│
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── products.csv
│   ├── reviews.csv
│   └── README.md
│
├── init-db/
│   └── init.sh
│
├── logs/
│
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

##### Running the Project ######

Install:

* Docker Desktop
* Git

Make sure Docker Desktop is running.

### Clone the repository

```bash
git clone https://github.com/stefanvlad616-cmyk/Project-Ecommerce.git
cd Project-Ecommerce
```

### Configure environment variables

Create a `.env` file containing the PostgreSQL configuration required by Docker Compose.

Example:

```env
#PostgreSQL

POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

#Airflow

AIRFLOW_FERNET_KEY=generate_a_fernet_key
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=your_airflow_db_password
AIRFLOW_DB_NAME=airflow_db
AIRFLOW_SIMPLE_AUTH_MANAGER_USERS=admin:change_me
AIRFLOW_JWT_SECRET=generate_a_random_secret


```
Generate the Airflow security keys

1. Generate AIRFLOW_FERNET_KEY

Run:

python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

Copy the generated value into:

AIRFLOW_FERNET_KEY=

2. Generate AIRFLOW_JWT_SECRET

Run:

python -c "import secrets; print(secrets.token_urlsafe(32))"

Copy the generated value into:

AIRFLOW_JWT_SECRET=

Configure the Airflow database

These values define the database and user that Airflow uses for its metadata database:

AIRFLOW_DB_USER=
AIRFLOW_DB_PASSWORD=
AIRFLOW_DB_NAME=

(here you can choose your credentials for each one)

The same values are used by init-db/init.sh to create the Airflow database and database user.

The project also uses simple_auth_manager_passwords.json.generated as a local password file. This file is intentionally excluded from Git using .gitignore.
So you have 2 options:
1.Run command "docker compose exec airflow-webserver cat /opt/airflow/simple_auth_manager_passwords.json.generated" everytime to see your generated password
2.My simple technique, by creating a file named "simple_auth_manager_passwords.json.generated" in the project folder, and set you password before airflow will create one;
-inside the file, paste this {"your_username": "your_password"} and put the desired credentials; username should be the one from AIRFLOW_SIMPLE_AUTH_MANAGER_USERS.
  
Also for a more detailed description about every task, create an empty folder in the project named logs; it's already set up so it will fill by itself with details.

### Start the project

```bash
docker compose up -d --build
```

Check the running containers:

```bash
docker compose ps
```

After the first build, next times yoo can run it by just "docker compose up -d"

Airflow should then be available through the configured webserver port.


## Data Quality & Error Handling

The project intentionally separates valid and invalid data.

Instead of silently deleting invalid records, the pipeline:

1. Detects the problem
2. Adds the validation reason
3. Records when the problem was detected
4. Fails the pipeline when the quality rule is violated

This approach makes data quality problems visible and traceable.


## Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* ETL / ELT pipelines
* Workflow orchestration
* SQL transformations
* Relational data modeling
* Data quality validation
* Error handling
* Data quarantine
* Analytical data marts
* Containerization
* Python data processing
* PostgreSQL
* Airflow DAGs
* Business intelligence
* Git version control

## Author

**Stefan Vlad**

This project was developed as a hands-on Data Engineering portfolio project focused on building and documenting an end-to-end data pipeline.

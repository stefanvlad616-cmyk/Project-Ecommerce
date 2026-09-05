from datetime import datetime
import os

from airflow import DAG
from airflow.decorators import task

#tables with their primary keys
TABLES = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "products": "product_id",
    "reviews": "review_id",
}


REQUIRED_COLUMNS = {
    "customers": ["email"],
    "orders": ["customer_id", "order_date"],
    "order_items": ["order_id", "product_id", "quantity"],
    "products": ["product_name", "category"],
    "reviews": ["product_id", "customer_id", "rating"],
}

FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("reviews", "product_id", "products", "product_id"),
    ("reviews", "customer_id", "customers", "customer_id"),
]

POSITIVE_WORDS = [
    "good", "great", "excellent", "love", "amazing", "perfect",
    "recommend", "happy", "best", "awesome", "fast", "quality"
]
NEGATIVE_WORDS = [
    "bad", "poor", "terrible", "broken", "slow", "worst",
    "disappointed", "waste", "cheap", "defective", "late", "refund"
]




def get_engine():
    #---hard imports here to load faster
    from sqlalchemy import create_engine

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    db = os.environ["POSTGRES_DB"]
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")


with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # ---LOAD DATA---

    @task(task_id="load_data")
    def load_data():
        import pandas as pd

        engine = get_engine()

        for table_name in TABLES:
            df = pd.read_csv(f"/opt/airflow/data/{table_name}.csv")
            df.to_sql(table_name, con=engine, if_exists="replace", index=False)

        print("Data was loaded with success.")



    # ----VALIDATE DATA----

    @task(task_id="validate_data")
    def validate_data():
        import pandas as pd

        engine = get_engine()

#---EMPTY TABLES VERIFY
      
        for table_name in TABLES:
            query = f"SELECT COUNT(*) FROM {table_name}"
            df = pd.read_sql(query, engine)

            rows = df.iloc[0, 0]

            assert rows > 0, f"Table {table_name} has 0 rows."

#---NULL primary keys-----

        for table_name, id_column in TABLES.items():

            query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE {id_column} IS NULL
            """

            df = pd.read_sql(query, engine)
            null_keys = df.iloc[0, 0]

            assert null_keys == 0, (
                f"{table_name}.{id_column} "
                f"contains NULL values."
            )

#---DUPLICATE primary keys----
      
            query=f"""SELECT COUNT(*)
                        FROM (
                            SELECT {id_column}
                            FROM {table_name}
                            GROUP BY {id_column}
                            HAVING COUNT(*) > 1
                        ) AS duplicates
            """
            df=pd.read_sql(query,engine)
            dupli=df.iloc[0, 0]
            assert dupli==0, f"{table_name},{id_column}  have duplicates."

#----NULL in mandatory columns----
        
        for table_name,columns in REQUIRED_COLUMNS.items():
            for column_name in columns:
                query=f"""SELECT COUNT(*)
                            FROM {table_name}
                            WHERE {column_name} is NULL
                """
                df=pd.read_sql(query,engine)
                result=df.iloc[0, 0]
                assert result==0, f"{table_name},{column_name} has NULL's."

#---FOREIGN KEYS---
        for table_name,fk_column,ref_table,ref_column in FOREIGN_KEYS:
            query=f"""SELECT COUNT(*)
                        FROM {table_name} t
                        LEFT JOIN {ref_table} r
                        ON t.{fk_column}=r.{ref_column}
                        WHERE r.{ref_column} IS NULL                                                                 
            """
            df=pd.read_sql(query,engine)
            result=df.iloc[0, 0]
            assert result==0,(
                f"{table_name}.{fk_column} contains values that do not exist in {ref_table}.{ref_column}."

            )



#----QUANTITY> 0-----

        query = """
            SELECT COUNT(*)
            FROM order_items
            WHERE quantity <= 0
        """
        df = pd.read_sql(query, engine)
        invalid_quantity = df.iloc[0, 0]

        assert invalid_quantity == 0, (
            f"order_items.quantity contains {invalid_quantity} rows with value <= 0."
        )

#---PRICE >= 0---

        PRICE_COLUMNS = [
            ("order_items", "unit_price"),
            ("order_items", "line_total"),
            ("products", "unit_cost"),
            ("orders", "order_total"),
        ]

        for table_name, column_name in PRICE_COLUMNS:
            query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE {column_name} < 0
            """
            df = pd.read_sql(query, engine)
            negative_values = df.iloc[0, 0]

            assert negative_values == 0, (
                f"{table_name}.{column_name} contains {negative_values} rows with negative values."
            )

#---LINE_TOTAL == quantity * unit_price-----

        query = """
            SELECT COUNT(*)
            FROM order_items
            WHERE ABS(line_total - (quantity * unit_price)) > 0.01
        """
        df = pd.read_sql(query, engine)
        mismatched_totals = df.iloc[0, 0]

        assert mismatched_totals == 0, (
            f"order_items.line_total does not match quantity * unit_price for {mismatched_totals} rows."
        )

        print("Data was validated successfully.")

    #------Transform data------
    
    @task(task_id="transform_data")
    def transform_data():
        import pandas as pd
        engine = get_engine()
        query=f"""
            select o.order_id,
                o.order_date,
                c.customer_id,
                c.city,
                p.product_id,
                p.product_name,
                p.category,
                oi.quantity,
                oi.unit_price,
                oi.line_total,
                p.unit_cost,
                oi.line_total - (p.unit_cost * oi.quantity) AS profit
            from orders o
            join order_items oi
                on o.order_id=oi.order_id
            join products p
                on p.product_id=oi.product_id  
            join customers c
                on o.customer_id=c.customer_id             
        """
        df= pd.read_sql(query,engine)
        df.to_sql("fact_sales", con=engine, if_exists="replace", index=False)

#---Daily sales---

    @task(task_id="daily_sales")
    def daily_sales():
        import pandas as pd
        engine=get_engine()
        query="""select order_date,
                        count(distinct order_id) as total_orders,
                        sum(quantity) as total_items,
                        sum(line_total) as total_sales,
                        sum(profit) as total_profit
                    from fact_sales
                    group by order_date
                    order by order_date          
        """
        df=pd.read_sql(query,engine)  
        df.to_sql("daily_sales", con=engine, if_exists="replace", index=False)  

#---Weekday sales---

    @task(task_id="weekday_sales")
    def weekday_sales():
        import pandas as pd
        engine=get_engine()
        query="""SELECT 
                    EXTRACT(ISODOW FROM order_date::date) AS weekday_num,
                    TO_CHAR(order_date::date, 'FMDay') AS weekday,
                    COUNT(DISTINCT order_id) AS total_orders,
                    SUM(line_total) AS total_sales,
                    SUM(profit) AS total_profit
                FROM fact_sales
                GROUP BY 
                    weekday_num,
                    weekday
                ORDER BY 
                    weekday_num;
        """
        df=pd.read_sql(query,engine)
        df.to_sql("weekday_sales", con=engine, if_exists="replace", index=False)

#------Category sales-------

    @task(task_id="category_sales")
    def category_sales():
        import pandas as pd
        engine=get_engine()
        query="""select category,
                        sum(quantity) as total_items,
                        sum(line_total) as total_sales,
                        sum(profit) as total_profit
                    from fact_sales
                    group by category
                    order by total_sales desc
        """
        df=pd.read_sql(query,engine)
        df.to_sql("category_sales", con=engine, if_exists="replace", index=False)

#---Reviews analysis----
  
    @task(task_id="reviews_analysis")
    def reviews_analysis():
        import pandas as pd
        engine = get_engine()

        query="""SELECT r.review_id, 
                        r.product_id, 
                        p.product_name, 
                        r.review_text 
                    FROM reviews r 
                    LEFT JOIN products p 
                    ON r.product_id = p.product_id
        """

        df = pd.read_sql(query, engine)

        def score_text(text):
            if not isinstance(text, str) or not text.strip():
                return 0
            text_lower = text.lower()
            pos = sum(text_lower.count(w) for w in POSITIVE_WORDS)
            neg = sum(text_lower.count(w) for w in NEGATIVE_WORDS)
            return pos - neg

        df["review_score"] = df["review_text"].apply(score_text)

        def label(score):
            if score > 0:
                return "positive"
            elif score < 0:
                return "negative"
            return "neutral"

        df["review_label"] = df["review_score"].apply(label)

        summary = (
            df.groupby(["product_id", "product_name"])
            .agg(
                total_reviews=("review_id", "count"),
                avg_sentiment_score=("review_score", "mean"),
                positive_reviews=("review_label", lambda x: (x == "positive").sum()),
                negative_reviews=("review_label", lambda x: (x == "negative").sum()),
                neutral_reviews=("review_label", lambda x: (x == "neutral").sum()),
            )
            .reset_index()
        )

        summary["pct_positive"] = (summary["positive_reviews"] / summary["total_reviews"] * 100).round(1)
        summary["pct_negative"] = (summary["negative_reviews"] / summary["total_reviews"] * 100).round(1)

        summary.to_sql("reviews_analysis", con=engine, if_exists="replace", index=False)
        print("Review analytics computed successfully.")


    # ----DEPENDENCIES-----

    load = load_data()
    validate = validate_data()
    transform = transform_data()
    daily_sales = daily_sales()
    category_sales = category_sales()
    weekday_sales = weekday_sales()
    reviews_analysis = reviews_analysis()
    load >> validate >> transform >> [daily_sales, category_sales, weekday_sales, reviews_analysis]
import pandas as pd
from sqlalchemy import create_engine, text


class DataUtils:
    def __init__(self):
        self.mysql_engine = create_engine(
            'mysql+pymysql://mysql_user:password@localhost:3307/oltp_database')
        self.staging_engine = create_engine(
            'postgresql+psycopg2://postgres_user:password@localhost:5433/StagingArea')
        self.dw_engine = create_engine(
            'postgresql+psycopg2://postgres_user:password@localhost:5433/DataWarehouse')

    def __fetch_latest_order_id(self):
        query = text(
            "SELECT COALESCE(MAX(order_id), 0) FROM DataWarehouse.fact_orders")
        with self.dw_engine.connect() as conn:
            result = conn.execute(query)
            return result.scalar()

    def get_newer_records(self):
        latest_order_id = self.__fetch_latest_order_id()

        # Fetch new orders
        query_orders = f"SELECT * FROM orders WHERE order_id > {latest_order_id}"
        new_orders = pd.read_sql(query_orders, self.mysql_engine)

        # Fetch user_ids from Data Warehouse
        query_existing_customers = "SELECT user_id FROM DataWarehouse.dim_customers"
        existing_customers = pd.read_sql(
            query_existing_customers, self.dw_engine)
        existing_customer_ids = existing_customers['user_id'].tolist()

        # Fetch new customers (those not in Data Warehouse)
        if existing_customer_ids:
            query_customers = f"""
                SELECT * FROM customers
                WHERE user_id NOT IN ({','.join(map(str, existing_customer_ids))})
            """
        else:
            query_customers = "SELECT * FROM customers"

        new_customers = pd.read_sql(query_customers, self.mysql_engine)

        # Store in staging area
        new_orders.to_sql('orders_staging', self.staging_engine,
                          if_exists='replace', index=False)
        new_customers.to_sql(
            'customers_staging', self.staging_engine, if_exists='replace', index=False)

    def transform_and_load_data(self):
        # Load data from staging area
        orders_staging = pd.read_sql(
            "SELECT * FROM orders_staging", self.staging_engine)
        customers_staging = pd.read_sql(
            "SELECT * FROM customers_staging", self.staging_engine)

        # Fetch product_ids from Data Warehouse
        query_existing_products = "SELECT product_id FROM DataWarehouse.dim_products"
        existing_products = pd.read_sql(
            query_existing_products, self.dw_engine)
        existing_product_ids = existing_products['product_id'].tolist()

        # Fetch new products (those not in Data Warehouse)
        new_product_ids = orders_staging['product_id'].unique()
        missing_product_ids = [
            pid for pid in new_product_ids if pid not in existing_product_ids]

        if missing_product_ids:
            query_products = f"""
                SELECT * FROM products
                WHERE product_id IN ({','.join(f"'{pid}'" for pid in missing_product_ids)})
            """
            new_products = pd.read_sql(query_products, self.mysql_engine)
            new_products.to_sql('dim_products', self.dw_engine,
                                schema='datawarehouse', if_exists='append', index=False)

        # Insert new customers into dim_customers
        customers_staging.to_sql('dim_customers', self.dw_engine,
                                 schema='datawarehouse', if_exists='append', index=False)

        # Transform orders and insert into fact_orders
        fact_orders = orders_staging[[
            'order_id', 'order_date', 'user_id', 'product_id', 'quantity', 'total_sales']]
        fact_orders.to_sql('fact_orders', self.dw_engine,
                           schema='datawarehouse', if_exists='append', index=False)

        # Truncate staging tables
        with self.staging_engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE orders_staging"))
            conn.execute(text("TRUNCATE TABLE customers_staging"))

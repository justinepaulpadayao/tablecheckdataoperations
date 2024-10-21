import duckdb
import pandas as pd
import os


def connect_to_duckdb():
    return duckdb.connect(':memory:')


def load_raw_data(con, file_path):
    con.execute(f"CREATE TABLE raw_data AS SELECT * FROM read_csv_auto('{file_path}')")


def clean_and_transform_data(con):
    con.execute("""
        CREATE TABLE cleaned_data AS
        SELECT 
            restaurant_names,
            food_names,
            first_name,
            CAST(food_cost AS DECIMAL(10,2)) AS food_cost
        FROM raw_data
        WHERE restaurant_names IS NOT NULL
          AND food_names IS NOT NULL
          AND first_name IS NOT NULL
          AND food_cost IS NOT NULL
    """)


def create_restaurant_stats(con):
    con.execute("""
        CREATE TABLE restaurant_stats AS
        SELECT 
            restaurant_names,
            COUNT(DISTINCT first_name) AS customer_count,
            SUM(food_cost) AS total_revenue
        FROM cleaned_data
        GROUP BY restaurant_names
    """)


def create_popular_dishes(con):
    con.execute("""
        CREATE TABLE popular_dishes AS
        SELECT 
            restaurant_names,
            food_names,
            COUNT(*) as order_count,
            ROW_NUMBER() OVER (PARTITION BY restaurant_names ORDER BY COUNT(*) DESC) as popularity_rank
        FROM cleaned_data
        GROUP BY restaurant_names, food_names
        QUALIFY popularity_rank <= 5
        ORDER BY restaurant_names, order_count DESC
    """)


def create_profitable_dishes(con):
    con.execute("""
        CREATE TABLE profitable_dishes AS
        SELECT 
            restaurant_names,
            food_names,
            SUM(food_cost) as total_revenue,
            ROW_NUMBER() OVER (PARTITION BY restaurant_names ORDER BY SUM(food_cost) DESC) as profit_rank
        FROM cleaned_data
        GROUP BY restaurant_names, food_names
        QUALIFY profit_rank <= 5
        ORDER BY restaurant_names, total_revenue DESC
    """)


def create_customer_stats(con):
    con.execute("""
        CREATE TABLE customer_stats AS
        SELECT 
            first_name,
            COUNT(DISTINCT restaurant_names) AS restaurants_visited,
            FIRST(restaurant_names) FILTER (WHERE rank = 1) AS most_visited_restaurant
        FROM (
            SELECT 
                first_name,
                restaurant_names,
                COUNT(*) AS visit_count,
                ROW_NUMBER() OVER (PARTITION BY first_name ORDER BY COUNT(*) DESC) AS rank
            FROM cleaned_data
            GROUP BY first_name, restaurant_names
        )
        GROUP BY first_name
        ORDER BY restaurants_visited DESC
    """)


def create_frequent_visitors(con):
    con.execute("""
        CREATE TABLE frequent_visitors AS
        SELECT 
            restaurant_names,
            first_name AS most_frequent_visitor,
            visit_count
        FROM (
            SELECT 
                restaurant_names,
                first_name,
                COUNT(*) AS visit_count,
                ROW_NUMBER() OVER (PARTITION BY restaurant_names ORDER BY COUNT(*) DESC) AS rank
            FROM cleaned_data
            GROUP BY restaurant_names, first_name
        )
        WHERE rank = 1
        ORDER BY visit_count DESC
    """)


def export_to_csv(con, table_name, output_path):
    con.execute(f"COPY {table_name} TO '{output_path}' (HEADER, DELIMITER ',')")


def main():
    try:
        con = connect_to_duckdb()
        load_raw_data(con, '../data/data.csv')
        clean_and_transform_data(con)
        create_restaurant_stats(con)
        create_popular_dishes(con)
        create_profitable_dishes(con)
        create_customer_stats(con)
        create_frequent_visitors(con)

        os.makedirs('tmp', exist_ok=True)
        export_to_csv(con, 'restaurant_stats', 'tmp/restaurant_stats.csv')
        export_to_csv(con, 'popular_dishes', 'tmp/popular_dishes.csv')
        export_to_csv(con, 'profitable_dishes', 'tmp/profitable_dishes.csv')
        export_to_csv(con, 'customer_stats', 'tmp/customer_stats.csv')
        export_to_csv(con, 'frequent_visitors', 'tmp/frequent_visitors.csv')

        print("Data transformation complete. CSV files exported for BigQuery import.")
    except Exception as e:
        print(f"An error occurred during data transformation: {str(e)}")
    finally:
        if 'con' in locals():
            con.close()


if __name__ == "__main__":
    main()

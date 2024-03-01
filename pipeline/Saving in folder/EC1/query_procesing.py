import duckdb
from collections import defaultdict
import hashlib
import pandas as pd


key_words = {1: 'select l_returnflag', 2: 's_acctbal', 3: 'select l_orderkey', 4: 'o_orderpriority', 5: 'n_name',
             6: 'sum(l_extendedprice', 7: 'supp_nation', 8: 'o_year', 9: 'sum_profit', 10: 'c_address',
             11: 'ps_partkey', 12: 'l_shipmode', 13: 'c_count', 14: 'p_type', 15: 'l_suppkey', 16: 'p_brand',
             17: 'l_extendedprice', 18: 'c_custkey', 19: 'l_discount', 20: 's_address', 21: 'numwait', 22: 'cntrycode'}
class QueryProcessor:

    def __init__(self, table_name = f'STL_QUERYTEXT_combined',
                csv_file_path = f'./dataset/STL_QUERYTEXT/combined_stl_querytext.csv',
                df: pd.DataFrame = None):
        self.df = df
        self.table_name = table_name
        self.csv_file_path = csv_file_path
        self.QUERY_HISTORY = []
    def find_query_instance_count(self, table, df):
        duckdb.sql(f"CREATE TABLE {table} AS SELECT * FROM df")
        # insert into the table "my_table" from the DataFrame "my_df"
        duckdb.sql(f"INSERT INTO {table} SELECT * FROM df")
        # print(f"df {df}")
        query = f"""SELECT column7 AS count
                    FROM {table}
                    WHERE column1 >= 100
                    AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%padb_fetch_sample:%'
                    AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%Alter Distkey::%'
                    AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%CREATE TEMP TABLE%'
                    AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%Alter Distkey:%'
                    AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%ALTER COLUMN ENCODE TYPE:%'
                    GROUP BY column7;
                    """
        results = duckdb.sql(query)
        # print(results)
        for result in results.fetchall():

            hashed_str = hashlib.sha256(result[0].encode("utf-8")).hexdigest()
            if hashed_str not in self.QUERY_HISTORY:
                # print("Identified more unique queries")
                self.QUERY_HISTORY.append(hashed_str)
        self.delete_table(duckdb, table_name=table)

    def find_query_template_counts(self, duckdb):
        """ Returns a dictionary with the query number (1 to 22) and the corresponding number of instances"""
        query_counts = defaultdict(int)
        print(self.df.head())
        for i in range(1, 23):
            if i == 5:
                unique_queries_query = f"""SELECT LEFT(column6, POSITION('FROM' in column6) - 1)  FROM {self.table_name}
                                           WHERE LEFT(column6, POSITION('FROM' in column6) - 1) LIKE '%{key_words[5]}%'
                                           AND LEFT(column6, POSITION('FROM' in column6) - 1) NOT LIKE '%padb_fetch_sample:%'
                                           AND LEFT(column6, POSITION('FROM' in column6) - 1) NOT LIKE '%{key_words[1]}%';
                                        """
            else:
                unique_queries_query = f"""SELECT LEFT(column6, POSITION('FROM' in column6) - 1)  FROM {self.table_name}
                                           WHERE LEFT(column6, POSITION('FROM' in column6) - 1) LIKE '%{key_words[i]}%'
                                           AND LEFT(column6, POSITION('FROM' in column6) - 1) NOT LIKE '%padb_fetch_sample:%';
                                        """
            result = duckdb.sql(unique_queries_query)
            query_counts[i] = len(result)
        return query_counts

    def get_unique_query_counts(self):
        duckdb_table = None
        self.table_name = 'my_table'
        if self.df is not None:
            # Register the DataFrame directly as a DuckDB table
            df = self.df
            duckdb.sql("CREATE TABLE my_table AS SELECT * FROM df")
            # insert into the table "my_table" from the DataFrame "my_df"
            duckdb.sql(f"INSERT INTO my_table SELECT * FROM df")
            # create_table_query = f"CREATE TABLE {table_name} AS SELECT * FROM df"
            # duckdb.sql(create_table_query)
        else:
            # Connect to DuckDB in-memory
            conn = duckdb.connect(database=':memory:', read_only=False)

            # Convert DataFrame to DuckDB table
            duckdb_table = conn.from_df(self.df, self.table_name)

            # Save DuckDB table
            conn.register(self.table_name, duckdb_table)

            # create_table_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file_path}')"
            # duckdb.sql(create_table_query)
        query_counts_dic = self.find_query_template_counts(duckdb)
        # print(f'{query_counts_dic = }')
        total_unique_queries = 0
        for query_type, num_instances in query_counts_dic.items():
            if num_instances > 0:
                total_unique_queries += 1
        # if duckdb_table is not None:
        #     self.delete_table(duckdb_table, table_name)
        print("starting to delete the table")
        self.delete_table(duckdb, self.table_name)
        return total_unique_queries

    def delete_table(self, duckdb, table_name):
        # Drop the table
        # print(f"{table_name} deleted!")
        duckdb.sql(f"DROP TABLE IF EXISTS {table_name}")

    def get_total_query_run_time(self, table):
        duckdb.sql(f"CREATE TABLE {table} AS SELECT * FROM df")
        # insert into the table "my_table" from the DataFrame "my_df"
        duckdb.sql(f"INSERT INTO {table} SELECT * FROM df")

        query = f"""SELECT column7 AS count
                           FROM {table}
                           WHERE column1 >= 100
                           AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%padb_fetch_sample:%'
                           AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%Alter Distkey::%'
                           AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%CREATE TEMP TABLE%'
                           AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%Alter Distkey:%'
                           AND LEFT(column7, POSITION('FROM' in column7) - 1) NOT LIKE '%ALTER COLUMN ENCODE TYPE:%'
                           GROUP BY column7;
                           """
        results = duckdb.sql(query)
        # print(results)
        for result in results.fetchall():

            hashed_str = hashlib.sha256(result[0].encode("utf-8")).hexdigest()
            if hashed_str not in self.QUERY_HISTORY:
                # print("Identified more unique queries")
                self.QUERY_HISTORY.append(hashed_str)
        self.delete_table(duckdb, table_name=table)
    def get_query_run_time(self):
        pass

if __name__ == '__main__':
    csv_file_path = './dataset/STL_QUERY/sample.csv'
    df = pd.read_csv(csv_file_path)
    qp = QueryProcessor(csv_file_path='./dataset/STL_QUERY/sample.csv', df=df)
    num_unique_queries = qp.get_unique_query_counts()
    print(num_unique_queries)

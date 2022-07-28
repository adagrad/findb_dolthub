import logging
import os
import random

from modules.dolt_api import dolt_load_file

log = logging.getLogger(__name__)


def df_to_csv(df, file):
    log.info(f"save dataframe to {file}")
    if os.path.exists(file):
        with open(file, 'a') as f:
            df.to_csv(f, header=False, index=False)
    else:
        df.to_csv(file, header=True, index=False)


def df_to_sql_with_replace(df, table_name, db_conn):
    def mysql_replace_into(table, conn, keys, data_iter):
        from sqlalchemy.ext.compiler import compiles
        from sqlalchemy.sql.expression import Insert

        @compiles(Insert)
        def replace_string(insert, compiler, **kw):
            s = compiler.visit_insert(insert, **kw)
            s = s.replace("INSERT INTO", "REPLACE INTO")
            return s

        data = [dict(zip(keys, row)) for row in data_iter]

        conn.execute(table.table.insert(replace_string=""), data)

    df.to_sql(table_name, db_conn, if_exists='append', method=mysql_replace_into)


def save_results(repo_database, df, load_into_dolt, table_name=None, csvfile=None, clear_afterwards=False):
    has_server = repo_database is not None and "://" in repo_database

    if csvfile is not None:
        df_to_csv(df, csvfile)

    if load_into_dolt:
        if has_server:
            try:
                df_to_sql_with_replace(df, table_name, repo_database)
            except Exception as e:
                if csvfile is None:
                    csvfile = f"{random.random()}.csv"
                    df_to_csv(df, csvfile)

                log.error(f"failed to insert DataFrame into table {table_name} dumped data to file {csvfile}\n{e}")
        else:
            assert csvfile is not None and table_name is not None, "csv file and table name required"
            rc, std, err = dolt_load_file(table_name, csvfile)
            if rc != 0: raise IOError(std + '\n' + err)

            if clear_afterwards:
                try:
                    os.unlink(csvfile)
                except Exception as e:
                    log.error(f"failed to delete file {csvfile}", e)

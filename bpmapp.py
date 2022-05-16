import streamlit as st
from connector import get_snowflake_connector


def run():
    st.header("Business Process Mining")

    ctx, cs = get_snowflake_connector()  # context, cursor
    try:
        cs.execute("SELECT * FROM mysnow.bpm.eventlog LIMIT 100")
        df = cs.fetch_pandas_all()
        st.write(df)
    finally:
        cs.close()
    ctx.close()


if __name__ == '__main__':
    run()

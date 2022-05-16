import streamlit as st
import connector


def run():
    st.header("Business Process Mining")

    ctx, cs = connector.get_snowflake_connector()  # context, cursor
    try:
        cs.execute("SELECT * FROM mysnow.bpm.eventlog ORDER BY _time LIMIT 100")
        df = cs.fetchall()
        st.write(df)
    finally:
        cs.close()
    ctx.close()


if __name__ == '__main__':
    run()

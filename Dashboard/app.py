import streamlit as st
import pandas as pd
import snowflake.connector

# 1️⃣ Snowflake connection
conn = snowflake.connector.connect(
    user='RESHMI',
    password='Reshmikas231104',
    account='NBBKTGA-ZF18921',  # e.g., xyz12345.us-east-1
    warehouse='COMPUTE_WH',
    database='HOTEL_DB',
    schema='PUBLIC'
)

 #2️⃣ Function to run query and return DataFrame
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
    return df

st.title("Hotel Revenue Dashboard")

# 3️⃣ Monthly Revenue Chart
monthly_query = """
SELECT
    DATE_TRUNC('month', TRY_TO_DATE(DATE, 'MM/DD/YYYY')) AS MONTH,
    SUM(TOTAL_REVENUE) AS REVENUE
FROM HOTEL_DB.PUBLIC.HOTEL_REVENUE
WHERE TRY_TO_DATE(DATE, 'MM/DD/YYYY') IS NOT NULL
GROUP BY DATE_TRUNC('month', TRY_TO_DATE(DATE, 'MM/DD/YYYY'))
ORDER BY MONTH;
"""
monthly_df = run_query(monthly_query)
monthly_df['REVENUE'] = monthly_df['REVENUE'].astype(float)  # Convert decimal → float
st.subheader("Monthly Revenue")
st.line_chart(monthly_df.set_index('MONTH'))

# 4️⃣ Revenue by City Chart
city_query = """
SELECT 
    HOTEL_CITY,
    SUM(TOTAL_AMOUNT) AS CITY_REVENUE
FROM HOTEL_DB.PUBLIC.HOTEL_BOOKINGS_RAW
WHERE TRY_TO_DATE(CHECK_IN_DATE, 'MM/DD/YYYY') IS NOT NULL
GROUP BY HOTEL_CITY
ORDER BY CITY_REVENUE DESC;
"""
city_df = run_query(city_query)
city_df['CITY_REVENUE'] = city_df['CITY_REVENUE'].astype(float)
st.subheader("Revenue by City")
st.bar_chart(city_df.set_index('HOTEL_CITY'))

# 5️⃣ Booking Status Chart
status_query = """
SELECT 
    BOOKING_STATUS,
    COUNT(*) AS TOTAL_BOOKINGS
FROM HOTEL_DB.PUBLIC.HOTEL_BOOKINGS_RAW
GROUP BY BOOKING_STATUS
ORDER BY TOTAL_BOOKINGS DESC;
"""
status_df = run_query(status_query)
status_df['TOTAL_BOOKINGS'] = status_df['TOTAL_BOOKINGS'].astype(int)
st.subheader("Booking Status")
st.bar_chart(status_df.set_index('BOOKING_STATUS'))

# 6️⃣ Room Type Revenue Chart
room_query = """
SELECT 
    ROOM_TYPE,
    SUM(TOTAL_AMOUNT) AS ROOM_REVENUE
FROM HOTEL_DB.PUBLIC.HOTEL_BOOKINGS_RAW
WHERE TRY_TO_DATE(CHECK_IN_DATE, 'MM/DD/YYYY') IS NOT NULL
GROUP BY ROOM_TYPE
ORDER BY ROOM_REVENUE DESC;
"""
room_df = run_query(room_query)
room_df['ROOM_REVENUE'] = room_df['ROOM_REVENUE'].astype(float)
st.subheader("Revenue by Room Type")
st.bar_chart(room_df.set_index('ROOM_TYPE'))
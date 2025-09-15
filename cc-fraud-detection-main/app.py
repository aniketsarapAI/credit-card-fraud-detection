import time  # to simulate a real time data, time loop
import json
import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import plotly.express as px  # interactive charts
import streamlit as st  # 🎈 data web app development
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from json import loads
import ast


# config for streamlit
st.set_page_config(
    page_title="Real-Time Fraud Detection Dashboard",
    page_icon="✅",
    layout="wide",
)

# dashboard title
st.title("Real-Time Fraud Detection")
placeholder = st.empty()

# Consuming data from prediction topic
consumer = KafkaConsumer(
    'prediction',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True)

# reading it and populating on a dashboard
count_not_fraud = 0
count_fraud = 0
total=0
final_df=pd.DataFrame()
for message in consumer:
    message = message.value
    result=ast.literal_eval(str(message.decode("utf-8")))
    if result["prediction"]==0:
        count_not_fraud+=1
        total+=1
    else:
        count_fraud+=1
        total += 1

    with placeholder.container():
        # create three columns
        kpi1, kpi2, kpi3 = st.columns(3)

        # fill in those three columns with respective metrics or KPIs
        kpi1.metric(
            label="Fraud ⏳",
            value=round(count_fraud),
        )

        kpi2.metric(
            label="Not Fraud ⏳",
            value=round(count_not_fraud),
        )

        kpi3.metric(
            label="Total Transaction ⏳",
            value=round(total),
        )

        cons_df=pd.DataFrame(result)
        final_df=pd.concat([cons_df,final_df], ignore_index=True)
        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            st.markdown("### Amount vs Prediction")
            fig = px.scatter(final_df, x="Amount", y="prediction")
            fig.update_layout(
                yaxis=dict(
                    tickvals=[0, 1]
                )
            )
            st.write(fig)

        with fig_col2:
            st.markdown("### Pie Chart")
            fig2 = px.pie(final_df, values='Amount', names='prediction')
            st.write(fig2)


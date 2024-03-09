import streamlit as st
import json
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("Titanic Survivor Predictor")

age = st.slider("Age", 0, 100, 1)
print(age)

# data = st.slider("Test", 0, 5, 1)
# print(data)

option = st.selectbox(
   "Sex",
   ("Male", "Female"),
   index=None,
   placeholder="Select Sex",
)
st.write('You selected:', option)

if st.button("Predict"):
    # read model
    # make prediction

    keyfile_bigquery = "/workspaces/data-engineering-on-gcp/proj-product-titanic-survivor-prediction/airflow/mnt/dags/pim-titanic-load-gcs-to-bigquery-b6e08f465048.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )
    project_id = "aerobic-ward-410501"
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location="us-central1",
    )
    
    query = f"""
        select * from ml.predict(model `pim_titanic_12345.survivor_predictor_2`, (
                select '{option}' as Sex, {age} as Age
            )
        )
    """
    df = bigquery_client.query(query).to_dataframe()
    print(df.head())

    survived = df["predicted_label"][0]
    if survived:
        result = "Survived"
    else:
        result = "Died.. ☠️"

    st.write(result)
    # if True:
    #     st.subheader(f'This is good 👍')
    # else:
    #     st.subheader(f'This is bad 👎')

    # st.write("Results here")
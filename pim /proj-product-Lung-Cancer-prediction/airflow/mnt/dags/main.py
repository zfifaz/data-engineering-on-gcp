import streamlit as st
import json
from google.cloud import bigquery
from google.oauth2 import service_account

st.title("Lung Cancer Predictor")

age = st.slider("Age", 0, 100, 1)
print(age)

# data = st.slider("Test", 0, 5, 1)
# print(data)
Gendermapper = {"Male":"M","Female":"F"}
Boomapper = {False:1, True:2}

Gender = st.selectbox(
   "Gender",
   ("Male", "Female"),
   index=None,
   placeholder="Select Gender",
)

SMOKING = st.toggle(
    "SMOKING"
)
YELLOW_FINGERS = st.toggle(
    "YELLOW_FINGERS"
)

ANXIETY = st.toggle(
    "ANXIETY"
)

PEER_PRESSURE = st.toggle(
    "PEER_PRESSURE"
)

CHRONIC_DISEASE = st.toggle(
    "CHRONIC_DISEASE"
)

FATIGUE = st.toggle(
    "FATIGUE"
)

ALLERGY = st.toggle(
    "ALLERGY"
)

WHEEZING = st.toggle(
    "WHEEZING"
)

ALCOHOL_CONSUMING = st.toggle(
    "ALCOHOL_CONSUMING"
)

COUGHING = st.toggle(
    "COUGHING"
)

SHORTNESS_OF_BREATH = st.toggle(
    "SHORTNESS_OF_BREATH"
)

SWALLOWING_DIFFICULTY = st.toggle(
    "SWALLOWING_DIFFICULTY"
)

CHEST_PAIN = st.toggle(
    "CHEST_PAIN"
)

if st.button("Predict"):
    # read model
    # make prediction

    keyfile_bigquery = "./lung-cancer-load-gcs-to-bigquery-b6e08f465048.json"
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
        select * from ml.predict(model `lung_cancer_09032024.cancer_predictor_1`
, (
                select '{Gendermapper[Gender]}' as GENDER, {age} as AGE,{Boomapper[SMOKING]} as SMOKING,	{Boomapper[YELLOW_FINGERS]} as YELLOW_FINGERS,	{Boomapper[ANXIETY]} as ANXIETY,{Boomapper[PEER_PRESSURE]} as PEER_PRESSURE,{Boomapper[CHRONIC_DISEASE]} as CHRONIC_DISEASE,	{Boomapper[FATIGUE]} as FATIGUE, 	{Boomapper[ALLERGY]} as ALLERGY,	{Boomapper[WHEEZING]} as WHEEZING,	{Boomapper[ALCOHOL_CONSUMING]} as ALCOHOL_CONSUMING,	{Boomapper[COUGHING]} as COUGHING,	{Boomapper[SHORTNESS_OF_BREATH]} as SHORTNESS_OF_BREATH,	{Boomapper[SWALLOWING_DIFFICULTY]} as SWALLOWING_DIFFICULTY,	{Boomapper[CHEST_PAIN]} as CHEST_PAIN
            )
        )
    """
    df = bigquery_client.query(query).to_dataframe()
    print(df.head())

    LUNG_CANCER = df["predicted_label"][0]


    st.write(LUNG_CANCER)
    # if True:
    #     st.subheader(f'This is good 👍')
    # else:
    #     st.subheader(f'This is bad 👎')

    # st.write("Results here")
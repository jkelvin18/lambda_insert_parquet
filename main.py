import json

import awswrangler as wr
import boto3
import pandas as pd

pathoutput = "s3://dlocalsotpagamentos/tb_riscos"


def extract_data(data):
    new_dict = {
        "anomesdia": data["result"][0]["database"],
        "codigo": data["result"][0]["atributo"][0]["atributoCodigo"],
        "tipo": data["result"][0]["atributo"][0]["atributoTipo"],
        "nome": data["result"][0]["atributo"][0]["atributoNome"],
        "valor": data["result"][0]["atributo"][0]["atributoValor"],
    }
    return pd.DataFrame.from_dict([new_dict])


def load_data(df, bucket, key):
    wr.s3.to_parquet(
        df=df,
        path=pathoutput,
        table="tb_riscos",
        database="db_local",
        compression="snappy",
        dataset=True,
        partition_cols=["anomesdia"],
    )


def lambda_handler(event, context):
    s3 = boto3.client("s3")

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    try:
        json_object = s3.get_object(Bucket=bucket, Key=key)
        data = json.load(json_object["Body"])

        df = extract_data(data)
        load_data(df, pathoutput)

        return "Success"
    except Exception as e:
        raise e

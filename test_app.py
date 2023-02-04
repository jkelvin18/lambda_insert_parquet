import json
import moto
import boto3
import pytest
import pandas as pd
from moto.s3.responses import DEFAULT_REGION_NAME
import os
import mock

# Setting the default region to us-east-1
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3_bucket_and_key():
    """Fixture to create a S3 bucket and key."""
    with moto.mock_s3():
        # Connect to the S3 resource
        conn = boto3.resource("s3", region_name=DEFAULT_REGION_NAME)

        # Create a bucket with the name test-bucket
        conn.create_bucket(Bucket="test-bucket")

        # Data to be stored in the S3 bucket
        data = {"result": [{"dataBase": "20220101",
                            "atributo": [{"atributoCodigo": "123",
                                          "atributoTipo": "test",
                                          "atributoNome": "test_attribute",
                                          "atributoValor": "test_value"}]}]}
        obj = conn.Object("test-bucket", "test-key")
        obj.put(Body=json.dumps(data))

        # Yield the bucket and key names
        yield "test-bucket", "test-key"


def test_extract_data(s3_bucket_and_key):
    """Test the extract_data function."""
    from main import extract_data

    # Get the bucket and key names from the fixture
    bucket, key = s3_bucket_and_key

    # Connect to the S3 resource
    s3 = boto3.resource("s3", region_name=DEFAULT_REGION_NAME)
    obj = s3.Object(bucket, key)

    # Get the data stored in the S3 bucket
    data = json.loads(obj.get()["Body"].read().decode("utf-8"))

    # Call the extract_data function and store the result in the df variable
    df = extract_data(data)

    # Assert that the result is a pandas DataFrame
    assert isinstance(df, pd.DataFrame)

    # Assert that the shape of the DataFrame is (1, 5)
    assert df.shape == (1, 5)

    # Assert that the column names of the DataFrame
    # are ['anomesdia', 'codigo, 'tipo', 'nome', 'valor']
    assert df.columns.tolist() == ["anomesdia",
                                   "codigo",
                                   "tipo",
                                   "nome",
                                   "valor"]

    # Assert that the value of the first row and first column is '20220101'
    assert df.iloc[0]["anomesdia"] == "20220101"


def test_load_data():
    """Test the load_data function."""
    df = pd.DataFrame(
        {
            "anomesdia": ["20220101"],
            "codigo": [1],
            "tipo": ["A"],
            "nome": ["Test"],
            "valor": [100],
        }
    )

    pathoutput = "s3://dlocalsotpagamentos/tb_riscos"

    with mock.patch("awswrangler.s3.to_parquet") as mock_to_parquet:
        from main import load_data
        load_data(df, pathoutput)
        mock_to_parquet.assert_called_once_with(
            df=df,
            path=pathoutput,
            table="tb_riscos",
            database="db_local",
            compression="snappy",
            dataset=True,
            partition_cols=["anomesdia"],
        )

import pandas as pd
import app.mock


from app.main import lambda_handler, extract_data, load_data


def test_extract_data():
    # Prepare test data
    data = {
        "result": [
            {
                "database": "20220101",
                "atributo": [
                    {
                        "atributoCodigo": 1,
                        "atributoTipo": "tipo1",
                        "atributoNome": "nome1",
                        "atributoValor": 10.0,
                    }
                ],
            }
        ]
    }

    # Call the function to be tested
    result = extract_data(data)

    # Assert the result
    assert result.iloc[0]["anomesdia"] == "20220101"
    assert result.iloc[0]["codigo"] == 1
    assert result.iloc[0]["tipo"] == "tipo1"
    assert result.iloc[0]["nome"] == "nome1"
    assert result.iloc[0]["valor"] == 10.0


def test_load_data():
    df = pd.DataFrame(
        {
            "anomesdia": ["20220101"],
            "codigo": [1],
            "tipo": ["A"],
            "nome": ["Test"],
            "valor": [100],
        }
    )

    bucket = "test-bucket"
    key = "test-key"

    # Mocking the wr.s3.to_parquet function so
    # that it does not write to S3 during the test
    with app.mock.patch("awswrangler.s3.to_parquet") as mock_to_parquet:
        load_data(df, bucket, key)
        mock_to_parquet.assert_called_once_with(
            df=df,
            path="s3://dlocalsotpagamentos/tb_riscos",
            table="tb_riscos",
            database="db_local",
            compression="snappy",
            dataset=True,
            partition_cols=["anomesdia"],
        )


def test_lambda_handler():
    # Prepare test data
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "bucket1"},
                    "object": {"key": "key1"},
                },
            },
        ],
    }

    # Call the function to be tested
    response = lambda_handler(event, None)

    # Assert the response
    assert response == "Success"


print("Hello World 2")

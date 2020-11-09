from egon.data.db import credentials


def test_egon_data_db_credentials():

    db_cred = credentials()

    assert "POSTGRES_DB" in db_cred.keys()
    assert "POSTGRES_USER" in db_cred.keys()
    assert "POSTGRES_PASSWORD" in db_cred.keys()
    assert "HOST" in db_cred.keys()
    assert "PORT" in db_cred.keys()

from egon.data.utils import egon_data_db_credentials


def test_egon_data_db_credentials():

    db_cred = egon_data_db_credentials()

    assert "POSTGRES_DB" in db_cred.keys()
    assert "POSTGRES_USER" in db_cred.keys()
    assert "POSTGRES_PASSWORD" in db_cred.keys()
    assert "HOST" in db_cred.keys()
    assert "PORT" in db_cred.keys()

import pytest

from weather_data_repository import Repository

def test_init():
    db_path = "/srv/weather_data_api/weather_data.sqlite"
    sut = Repository(db_path)

    assert sut

def test_create_connection_success():
    db_path = "/srv/weather_data_api/weather_data.sqlite"
    sut = Repository(db_path)

    result = sut.create_connection()

    assert result

def test_execute_query():
    db_path = "/srv/weather_data_api/weather_data.sqlite"
    sut = Repository(db_path)

    result, test = sut.execute_query(query="SELECT * FROM weather LIMIT 1")

    assert len(test) == 1

    assert result

def test_current_weather():
    db_path = "/srv/weather_data_api/weather_data.sqlite"
    sut = Repository(db_path)

    result, data = sut.current_weather()

    assert len(data) == 1

    assert result
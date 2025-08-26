from unittest.mock import patch, MagicMock
from datetime import datetime
from psycopg2 import sql
from requests import RequestException
from src.utils import get_vacancies, create_database, save_data_to_database


# Пример тестовых данных
MOCK_VACANCY = {
    "id": "123",
    "name": "Python Developer",
    "employer": {
        "id": "456",
        "name": "Company Inc",
        "url": "https://hh.ru/employer/456",
        "alternate_url": "https://company.ru",
        "logo_urls": {"original": "logo.png"}
    },
    "salary": {"from": 100000, "to": 150000, "currency": "RUR"},
    "url": "https://hh.ru/vacancy/123",
    "published_at": datetime.now().isoformat(),
    "snippet": {
        "requirement": "Опыт от 3 лет",
        "responsibility": "Разработка на Python"
    }
}

DB_PARAMS = {
    'dbname': 'test_db',
    'user': 'test_user',
    'password': 'test_pass',
    'host': 'localhost',
    'port': '5432'
}


@patch('requests.get')
def test_get_vacancies_success(mock_get):
    # Мокаем успешный ответ API
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"items": [MOCK_VACANCY]}
    mock_get.return_value = mock_response

    result = get_vacancies(["456"])
    assert len(result) == 1
    assert result[0]['name'] == 'Python Developer'
    assert result[0]['id'] == '123'


@patch('requests.get')
def test_get_vacancies_error(mock_get):
    # Мокаем ошибку сети
    mock_get.side_effect = RequestException("Network error")
    result = get_vacancies(["456"])
    assert result == []

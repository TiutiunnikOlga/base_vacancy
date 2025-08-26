import pytest
import psycopg2
import os
import configparser
from unittest.mock import patch, MagicMock
from psycopg2 import OperationalError
from src.classes import DBManager


# Создаем Моки для тестирования
class MockCursor:
    def __init__(self, fetch_result=None):
        self.fetch_result = fetch_result
        self.execute_calls = []

    def execute(self, query, params=None):
        self.execute_calls.append((query, params))

    def fetchall(self):
        return self.fetch_result

    def fetchone(self):
        return self.fetch_result[0] if self.fetch_result else None


class MockConnection:
    def __init__(self, cursor_result=None):
        self.cursor_result = cursor_result

    def cursor(self):
        return MockCursor(self.cursor_result)


# Создаем фикстуры
@pytest.fixture
def mock_db_manager(monkeypatch):
    monkeypatch.setattr(os.path, 'exists', lambda x: True)

    mock_config = MagicMock(spec=configparser.ConfigParser)
    mock_config.read.return_value = True

    mock_section = {
        'host': 'test_host',
        'port': '5432',
        'dbname': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }
    mock_config.__getitem__.return_value = mock_section

    monkeypatch.setattr(configparser, 'ConfigParser', lambda: mock_config)

    # Создаем экземпляр DBManager
    db_manager = DBManager('dummy_path')

    # Мокаем подключение к БД
    monkeypatch.setattr(psycopg2, 'connect', lambda **kwargs: MagicMock())

    return db_manager


def test_load_config(mock_db_manager):
    # Проверяем загрузку конфигурации
    result = mock_db_manager.load_config()
    assert result == mock_db_manager.params


def test_connect(mock_db_manager):
    # Проверяем подключение
    mock_db_manager.connect()
    assert mock_db_manager.connection.autocommit == True


def test_get_companies_and_vacancies_count(mock_db_manager):
    # Создаем моки для курсора
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    # Настраиваем поведение курсора
    mock_cursor.fetchall.return_value = [('Company1', 5), ('Company2', 3)]
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Устанавливаем моки в менеджер БД
    mock_db_manager.connection = mock_connection

    # Выполняем тест
    result = mock_db_manager.get_companies_and_vacancies_count(['1', '2'])
    expected = [
        {'company_name': 'Company1', 'vacancies_count': 5},
        {'company_name': 'Company2', 'vacancies_count': 3}
    ]

    assert result == expected


def test_get_all_vacancies(mock_db_manager):
    # Создаем моки для курсора
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    # Настраиваем возвращаемые данные
    mock_cursor.fetchall.return_value = [
        ('Company1', 'Python Developer', {'from': 100000}, 'url1'),
        ('Company2', 'Senior Developer', {'from': 150000}, 'url2')
    ]

    # Настраиваем контекстный менеджер
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Устанавливаем моки в менеджер БД
    mock_db_manager.connection = mock_connection

    # Выполняем тест
    result = mock_db_manager.get_all_vacancies()
    expected = [
        {
            'company_name': 'Company1',
            'vacancy_name': 'Python Developer',
            'salary': {'from': 100000},
            'url': 'url1'
        },
        {
            'company_name': 'Company2',
            'vacancy_name': 'Senior Developer',
            'salary': {'from': 150000},
            'url': 'url2'
        }
    ]

    assert result == expected


def test_get_avg_salary(mock_db_manager):
    # Создаем моки для курсора
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    # Настраиваем возвращаемые данные
    mock_cursor.fetchone.return_value = (120000,)

    # Настраиваем контекстный менеджер
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Устанавливаем моки в менеджер БД
    mock_db_manager.connection = mock_connection

    # Выполняем тест
    result = mock_db_manager.get_avg_salary()
    assert result == 120000.0


def test_get_vacancies_with_higher_salary(mock_db_manager):
    # Мокаем среднюю зарплату
    mock_db_manager.get_avg_salary = MagicMock(return_value=100000)

    # Создаем моки для курсора
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    # Настраиваем возвращаемые данные
    mock_cursor.fetchall.return_value = [
        ('Company1', 'Senior Python', {'from': 150000}, 'url1'),
        ('Company2', 'Team Lead', {'from': 200000}, 'url2')
    ]

    # Настраиваем контекстный менеджер
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Устанавливаем моки в менеджер БД
    mock_db_manager.connection = mock_connection

    # Выполняем тест
    result = mock_db_manager.get_vacancies_with_higher_salary()
    expected = [
        {
            'company_name': 'Company1',
            'vacancy_name': 'Senior Python',
            'salary': {'from': 150000},
            'url': 'url1'
        },
        {
            'company_name': 'Company2',
            'vacancy_name': 'Team Lead',
            'salary': {'from': 200000},
            'url': 'url2'
        }
    ]

    assert result == expected


def test_get_vacancies_with_keyword(mock_db_manager):
    # Создаем моки для курсора
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    # Настраиваем возвращаемые данные
    mock_cursor.fetchall.return_value = [
        ('Company1', 'Python Developer', {'from': 100000}, 'url1'),
        ('Company2', 'Python Team Lead', {'from': 150000}, 'url2')
    ]

    # Настраиваем контекстный менеджер
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    # Устанавливаем моки в менеджер БД
    mock_db_manager.connection = mock_connection

    # Выполняем тест
    result = mock_db_manager.get_vacancies_with_keyword('python')
    expected = [
        {
            'company_name': 'Company1',
            'vacancy_name': 'Python Developer',
            'salary': {'from': 100000},
            'url': 'url1'
        },
        {
            'company_name': 'Company2',
            'vacancy_name': 'Python Team Lead',
            'salary': {'from': 150000},
            'url': 'url2'
        }
    ]

    assert result == expected


def test_get_vacancies_with_empty_keyword(mock_db_manager):
    # Тестируем обработку пустого ключевого слова
    result = mock_db_manager.get_vacancies_with_keyword('')
    assert result == []


def test_get_vacancies_with_no_results(mock_db_manager):
    # Тестируем случай, когда вакансий не найдено
    mock_cursor = MockCursor([])
    mock_db_manager.connection = MockConnection(mock_cursor)

    result = mock_db_manager.get_vacancies_with_keyword('nonexistent')
    assert result == []


def test_invalid_config_file():
    # Тестируем обработку отсутствующего конфигурационного файла
    with pytest.raises(FileNotFoundError):
        DBManager('nonexistent_path.ini')


def test_no_connection():
    config_path = 'dummy_path'

    with patch.object(os.path, 'exists', return_value=True):
        with patch('configparser.ConfigParser') as mock_configparser:
            mock_config = mock_configparser.return_value
            mock_config.read.return_value = True

            mock_section = {
                'host': 'test_host',
                'port': '5432',
                'dbname': 'test_db',
                'user': 'test_user',
                'password': 'test_password'
            }
            mock_config.__getitem__.return_value = mock_section

            with patch('psycopg2.connect') as mock_connect:
                mock_connect.return_value = MagicMock()

                db_manager = DBManager(config_path)
                db_manager.connection = None

                try:
                    db_manager.get_all_vacancies()
                except Exception as e:
                    assert str(e) == "Отсутствует активное соединение с базой данных"

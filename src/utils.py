from typing import Any
import requests
from requests.exceptions import RequestException
import psycopg2
from psycopg2 import sql, extras
from psycopg2 import OperationalError
import json



def get_vacancies(org_ids: list[str], query: str="") -> list[dict[str, Any]]:
    """Получение данных о вакансиях интересующих организаций"""
    if not org_ids:
        print("Список организаций пуст")
        return []
    params = {
        "id": ",".join(org_ids),
        "text": query,
        "per_page": 100,  # Количество вакансий на странице
        "page": 0  # Начальная страница
    }

    try:
        response = requests.get("https://api.hh.ru/vacancies", params=params)
        response.raise_for_status()  # Проверяем успешность запроса
        return response.json().get('items', [])  # Возвращаем только список вакансий
    except RequestException as e:
        print(f"Ошибка при запросе к API: {e}")
        return []


def create_database(database_name: str, params: dict, org_ids: list[str] = []) -> None:
    """Создание базы данных для сохранения интересующих вакансий"""
    try:
        # Создаем копию параметров без dbname для подключения к postgres
        base_params = {k: v for k, v in params.items() if k != 'dbname'}

        # Устанавливаем соединение с базой данных postgres
        conn = psycopg2.connect(dbname='postgres', **base_params)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (database_name,)
        )

        if cur.fetchone():
            # Если база существует, удаляем её
            cur.execute(sql.SQL("DROP DATABASE {}").format(
                sql.Identifier(database_name)
            ))

        cur.execute(f'CREATE DATABASE {database_name}')
        cur.close()
        conn.close()

        # Обновляем параметры с новым именем базы данных
        params['dbname'] = database_name

        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                # Основная таблица вакансий
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS vacancies (
                        id SERIAL PRIMARY KEY,
                        hh_id VARCHAR(255) UNIQUE,
                        name VARCHAR(255),
                        employer_id VARCHAR(255),
                        salary JSONB,
                        description TEXT,
                        url VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        published_at TIMESTAMP
                    )
                ''')

                # Таблица работодателей
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS employers (
                        id VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255),
                        url VARCHAR(255),
                        alternate_url VARCHAR(255),
                        logo_urls JSONB
                    )
                ''')

                # Таблица требований и обязанностей
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS snippets (
                        vacancy_id INT PRIMARY KEY,
                        requirement TEXT,
                        responsibility TEXT,
                        FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
                    )
                ''')

    except OperationalError as e:
        print(f"Ошибка при создании базы данных: {e}")


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict) -> None:
    # Создаем копию параметров без dbname
    base_params = {k: v for k, v in params.items() if k != 'dbname'}

    try:
        # Передаем только необходимые параметры
        conn = psycopg2.connect(dbname=database_name, **base_params)
        conn.autocommit = True

        with conn.cursor() as cur:
            for vacancy in data:
                try:
                    # Сохраняем работодателя
                    employer = vacancy.get('employer', {})
                    employer_data = {
                        'id': employer.get('id'),
                        'name': employer.get('name'),
                        'url': employer.get('url'),
                        'alternate_url': employer.get('alternate_url'),
                        'logo_urls': json.dumps(employer.get('logo_urls', {}))
                    }

                    cur.execute('''
                    INSERT INTO employers (id, name, url, alternate_url, logo_urls)
                    VALUES (%(id)s, %(name)s, %(url)s, %(alternate_url)s, %(logo_urls)s)
                    ON CONFLICT (id) DO NOTHING
                    ''', employer_data)

                    # Сохраняем основную информацию о вакансии
                    vacancy_data = {
                        'hh_id': vacancy.get('id'),
                        'name': vacancy.get('name'),
                        'employer_id': employer.get('id'),
                        'salary': json.dumps(vacancy.get('salary', {})),
                        'url': vacancy.get('url'),
                        'published_at': vacancy.get('published_at')
                    }

                    cur.execute('''
                    INSERT INTO vacancies (hh_id, name, employer_id, salary, url, published_at)
                    VALUES (%(hh_id)s, %(name)s, %(employer_id)s, %(salary)s, %(url)s, %(published_at)s)
                    ON CONFLICT (hh_id) DO NOTHING
                    RETURNING id
                    ''', vacancy_data)
                    vacancy_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    # Сохраняем описание и требования
                    snippet = vacancy.get('snippet', {})
                    if vacancy_id:
                        cur.execute('''
                        INSERT INTO snippets (vacancy_id, requirement, responsibility)
                        VALUES (%s, %s, %s)
                        ''', (
                            vacancy_id,
                            snippet.get('requirement'),
                            snippet.get('responsibility')
                        ))
                except Exception as e:
                    print(f"Ошибка при обработке вакансии {vacancy.get('id')}: {str(e)}")

    except psycopg2.Error as e:
        print(f"Ошибка подключения к базе данных: {str(e)}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

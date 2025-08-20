import psycopg2
from psycopg2 import sql, extras, OperationalError
import json
import configparser
import os


class DBManager:
    def __init__(self, config_path: str):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Конфигурационный файл {config_path} не найден")
        self.config_path = config_path
        self.connection = None
        self.params = self.load_config()

    def load_config(self) -> dict:
        config = configparser.ConfigParser()
        try:
            config.read(self.config_path)
            section = 'postgresql'
            if section not in config:
                raise ValueError(f"Секция {section} не найдена в конфигурационном файле")

            params = {
                'host': config[section]['host'],
                'port': config[section]['port'],
                'dbname': config[section]['dbname'],
                'user': config[section]['user'],
                'password': config[section]['password'],
                'client_encoding': config[section].get('client_encoding', 'utf8')
            }
            return params
        except Exception as e:
            print(f"Ошибка при загрузке конфигурации: {e}")
            return {}

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.params)
            self.connection.autocommit = True
        except OperationalError as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def close(self):
        if self.connection:
            self.connection.close()

    def get_companies_and_vacancies_count(self, org_ids: list[str]) -> list[dict]:
        """Получает список всех компаний и количество вакансий у каждой компании"""
        placeholders = ', '.join(['%s'] * len(org_ids))

        query = f"""
            SELECT 
                e.name AS company_name,
                COUNT(v.id) AS vacancies_count
            FROM 
                employers e
            LEFT JOIN 
                vacancies v ON e.id = v.employer_id
            WHERE 
                e.id IN ({placeholders})
            GROUP BY 
                e.name
            ORDER BY 
                vacancies_count DESC
            """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, org_ids)
                result = cursor.fetchall()
                return [{'company_name': row[0], 'vacancies_count': row[1]} for row in result]
        except Exception as e:
            print(f"Ошибка при получении данных о компаниях: {e}")
            return []

    def get_all_vacancies(self) -> list[dict]:
        """Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты"""
        query = """
        SELECT 
            e.name AS company_name,
            v.name AS vacancy_name,
            v.salary,
            v.url
        FROM 
            vacancies v
        JOIN 
            employers e ON v.employer_id = e.id
        ORDER BY 
            company_name
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return [{
                    'company_name': row[0],
                    'vacancy_name': row[1],
                    'salary': row[2],
                    'url': row[3]
                } for row in result]
        except Exception as e:
            print(f"Ошибка при получении всех вакансий: {e}")
            return []

    def get_avg_salary(self) -> float:
        """Получает среднюю зарплату по всем вакансиям"""
        query = """
        SELECT 
            AVG((salary->>'from')::numeric) AS avg_salary
        FROM 
            vacancies
        WHERE 
            salary->>'from' IS NOT NULL
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return float(result[0]) if result[0] else 0.0
        except Exception as e:
            print(f"Ошибка при получении средней зарплаты: {e}")
            return 0.0

    def get_vacancies_with_higher_salary(self) -> list[dict]:
        """Получает список всех вакансий, у которых зарплата выше средней"""
        avg_salary = self.get_avg_salary()
        query = f"""
        SELECT 
            e.name AS company_name,
            v.name AS vacancy_name,
            v.salary,
            v.url
        FROM 
            vacancies v
        JOIN 
            employers e ON v.employer_id = e.id
        WHERE 
            ((v.salary->>'from')::numeric > {avg_salary})
        ORDER BY 
            (v.salary->>'from')::numeric DESC
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return [{
                    'company_name': row[0],
                    'vacancy_name': row[1],
                    'salary': row[2],  # Убрали json.loads
                    'url': row[3]
                }
                    for row in result]
        except Exception as e:
            print(f"Ошибка при получении вакансий с высокой зарплатой: {e}")
            return []

    def get_vacancies_with_keyword(self, keyword: str) -> list[dict]:
        """Получает список всех вакансий, в названии которых содержатся переданные слова"""
        query = """
        SELECT 
            e.name AS company_name,
            v.name AS vacancy_name,
            v.salary,
            v.url
        FROM 
            vacancies v
        JOIN 
            employers e ON v.employer_id = e.id
        WHERE 
            LOWER(v.name) LIKE %s
        ORDER BY 
            company_name
        """
        try:
            with self.connection.cursor() as cursor:
                # Используем % для поиска подстроки
                search_pattern = f"%{keyword.lower()}%"
                cursor.execute(query, (search_pattern,))
                result = cursor.fetchall()
                return [
                    {
                        'company_name': row[0],
                        'vacancy_name': row[1],
                        'salary': json.loads(row[2]),
                        'url': row[3]
                    }
                    for row in result
                ]
        except Exception as e:
            print(f"Ошибка при поиске вакансий по ключевому слову: {e}")
            return []

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

# Пример использования класса
if __name__ == "__main__":
    try:
        # Создаем экземпляр менеджера БД
        db_manager = DBManager(r'C:\Users\Olga\PycharmProjects\base_vacancy\database.ini')

        # Используем контекстный менеджер
        with db_manager as db:
            # Получение компаний и количества вакансий
            companies = db.get_companies_and_vacancies_count()
            print("Компании и количество вакансий:")
            print(companies)

            # Получение всех вакансий
            all_vacancies = db.get_all_vacancies()
            print("\nВсе вакансии:")
            print(all_vacancies)

            # Получение средней зарплаты
            avg_salary = db.get_avg_salary()
            print(f"\nСредняя зарплата: {avg_salary}")

            # Получение вакансий с зарплатой выше средней
            high_salary_vacancies = db.get_vacancies_with_higher_salary()
            print("\nВакансии с зарплатой выше средней:")
            print(high_salary_vacancies)

            # Получение вакансий по ключевому слову
            keyword_vacancies = db.get_vacancies_with_keyword("python")
            print("\nВакансии с ключевым словом 'python':")
            print(keyword_vacancies)
    except Exception as e:
        print(f"Произошла ошибка: {e}")

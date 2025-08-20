import os
from src.utils import create_database, get_vacancies, save_data_to_database
from config import config
from src.classes import DBManager
from pprint import pprint


def main():
    api_key = os.getenv("API_KEY")
    org_ids = [
        '5145508',  # MAK IT
        '976061',  # Altenar
        '9739596',  # UAB Cronex
        '10210282',  # Teyca
        '1993100',  # Гриндата
        '4527376',  # ВИГ Транс Лог
        '5810442',  # Udevs
        '9974038',  # Ozen Realty
        '10673143',  # ЧП KG Friend
        '9739183'   # СофтЭсАр
    ]
    params = config()

    data = get_vacancies(org_ids)
    create_database('vacancies', params)
    save_data_to_database(data, 'vacancies', params)
    db_manager = DBManager(r'C:\Users\Olga\PycharmProjects\base_vacancy\database.ini')

    # Используем контекстный менеджер
    with db_manager as db:
        # Получение компаний и количества вакансий
        companies = db.get_companies_and_vacancies_count(org_ids)
        print("Компании и количество вакансий:")
        pprint(companies)

        # Получение всех вакансий
        all_vacancies = db.get_all_vacancies()
        print("\nВсе вакансии:")
        pprint(all_vacancies)

        # Получение средней зарплаты
        avg_salary = db.get_avg_salary()
        print(f"\nСредняя зарплата: {avg_salary}")

        # Получение вакансий с зарплатой выше средней
        high_salary_vacancies = db.get_vacancies_with_higher_salary()
        print("\nВакансии с зарплатой выше средней:")
        pprint(high_salary_vacancies)

        # Получение вакансий по ключевому слову
        keyword_vacancies = db.get_vacancies_with_keyword("python")
        print("\nВакансии с ключевым словом 'python':")
        pprint(keyword_vacancies)

if __name__ == '__main__':
    main()
from src.utils import create_database, get_vacancies, save_data_to_database
from config import config
from src.classes import DBManager
from pprint import pprint


def format_salary(salary_dict):
    if not salary_dict:
        return "Не указана"

    from_amount = salary_dict.get('from', 'Не указано')
    to_amount = salary_dict.get('to', 'Не указано')
    currency = salary_dict.get('currency', 'Не указана')

    if from_amount == to_amount:
        return f"{from_amount} {currency}"
    else:
        return f"{from_amount} - {to_amount} {currency}"


def format_companies(companies):
    result = "\nСписок компаний и количество вакансий:\n"
    for company in companies:
        result += f"Компания: {company['company_name']} - {company['vacancies_count']} вакансий\n"
    return result


def format_vacancies(vacancies):
    result = "\nСписок всех вакансий:\n"
    for vacancy in vacancies:
        result += f"Компания: {vacancy['company_name']}\n"
        result += f"Вакансия: {vacancy['vacancy_name']}\n"
        result += f"Зарплата: {format_salary(vacancy['salary'])}\n"
        result += f"Ссылка: {vacancy['url']}\n"
        result += "-----------------------------------\n"
    return result

def main():
    '''Выбираем интересующие компании'''
    org_ids = [
        "5145508",  # MAK IT
        "976061",  # Altenar
        "9739596",  # UAB Cronex
        "10210282",  # Teyca
        "1993100",  # Гриндата
        "1781300",  # Дром
        "5810442",  # Udevs
        "124293331",  # EGAR
        "10673143",  # ЧП KG Friend
        "9739183",  # СофтЭсАр
    ]
    params = config()

    # Получаем все вакансии
    data = get_vacancies(org_ids)
    # Создаем базу данных
    create_database("vacancies", params)
    # Сохраняем полученные вакансии в таблицу
    save_data_to_database(data, "vacancies", params)
    db_manager = DBManager(r"C:\Users\Olga\PycharmProjects\base_vacancy\database.ini")

    # Используем контекстный менеджер
    with db_manager as db:
        # Получение компаний и количества вакансий
        companies = db.get_companies_and_vacancies_count(org_ids)
        print("Вы хотите посмотреть список избранных компаний? y/n")
        answer = input()
        if answer == 'y':
            print(format_companies(companies))

        # Получение всех вакансий
        all_vacancies = db.get_all_vacancies()
        print("Вы хотите посмотреть список всех вакансий? y/n")
        answer = input()
        if answer == 'y':
            print(format_vacancies(all_vacancies))

        # Получение средней зарплаты
        avg_salary = db.get_avg_salary()
        print("Вы хотите посмотреть среднюю зарплату? y/n")
        answer = input()
        if answer == 'y':
            print(f"\nСредняя зарплата по всем вакансиям: {avg_salary} рублей")

        # Получение вакансий с зарплатой выше средней
        high_salary_vacancies = db.get_vacancies_with_higher_salary()
        print("Вы хотите посмотреть список вакансий с зарплатой выше среднего? y/n")
        answer = input()
        if answer == 'y':
            print("\nВакансии с зарплатой выше средней:")
            print(format_vacancies(high_salary_vacancies))

        # Получение вакансий по ключевому слову
        print("Введите ключевое слово")
        keyword = input().strip()
        keyword_vacancies = db.get_vacancies_with_keyword(keyword)
        print(f"\nВакансии с ключевым словом {keyword}:")
        print(format_vacancies(keyword_vacancies))


if __name__ == "__main__":
    main()

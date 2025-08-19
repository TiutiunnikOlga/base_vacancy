import os
from src.utils import create_database, get_vacancies, save_data_to_database
from config import config


def main():
    api_key = os.getenv("API_KEY")
    org_ids = [
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        ""
    ]
    params = config()

    data = get_vacancies(org_ids)
    create_database('hh_vacancies', params)
    save_data_to_database(data, 'hh_vacancies', params)

if __name__ == '__main__':
    main()
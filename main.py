# coding: utf-8
import os
from dataclasses import dataclass
from dotenv import load_dotenv
import asyncio
import httpx
from bs4 import BeautifulSoup
import pandas as pd


load_dotenv("pj.env")


@dataclass(frozen=True)
class Contact:
    position: str
    name: str
    email: str


@dataclass(frozen=True)
class Hospital:
    name: str
    url: str
    homepage: str
    emails: list[Contact]
    faches: list[str]


@dataclass(frozen=True)
class University:
    name: str
    url: str
    emails: list[Contact]
    hospitals: list[Hospital]


async def get_uni_info(uni_url):
    async with httpx.AsyncClient() as client:
        response = await client.get(uni_url)
        soup = BeautifulSoup(response.text, "html.parser")

        name = soup.find(id='content_Fakultaet_bezeichnung').text.strip()
        emails = find_email(soup)
        hospitals_links = find_uni_hospitals(soup)
        hospitals = []
        for hospital in hospitals_links:
            hospitals.append(await get_hos_info(hospital))

        uni = University(name, uni_url, emails, hospitals)
        return uni


async def get_hos_info(hos_url):
    async with httpx.AsyncClient() as client:
        response = await client.get(hos_url)
        soup = BeautifulSoup(response.text, "html.parser")

        name = soup.find(id='content_Krankenhaus_bezeichnung').text.strip()
        try:
            homepage = soup.find(id='content_Krankenhaus_Webseiten').find('p').find('a').get('href')
        except AttributeError:
            homepage = 'Not Found'
        emails = find_email(soup)
        faches = [fach.text.strip() for fach in soup.find_all('span', class_='Fach')]
        hos = Hospital(name, hos_url, homepage, emails, faches)
        return hos


def find_email(soup):
    staff_list = []
    staff_divs = soup.find_all('div', class_='Position Stufe_0')
    for staff in staff_divs:
        position_element = staff.find('h3')
        name_element = staff.find('p', class_='Person Stufe_0')
        email_element = staff.find('a')

        if position_element and name_element and email_element:
            position = position_element.text.strip()
            name = name_element.text.strip()
            email = email_element.text.strip().replace(" (at) ", "@")

            person = Contact(position, name, email)
            staff_list.append(person)
    return staff_list


def find_uni_hospitals(soup):
    hospitals_container = soup.find(id='Fakultaet_Krankenhaeuser')
    hospitals_links = ['https://pj-portal.de/' + link.get('href') for link in hospitals_container.find_all('a')]
    return hospitals_links


def home_page_scrapper(url):
    response = httpx.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    links = []
    uni_wrappers = soup.find_all('div', class_='fakultaet_wrapper')
    for wrapper in uni_wrappers:
        link = 'https://pj-portal.de/' + wrapper.find('a').get('href')
        links.append(link)
    return links


def fach_search(uni_list, keywords):
    for keyword in keywords:
        uni_cache = []

        uni_emails_df = []
        hos_emails_df = []
        hos_links_df = []

        for uni in uni_list:
            for hos in uni.hospitals:
                if keyword in hos.faches:
                    # Get university emails
                    if uni.name not in uni_cache:
                        uni_cache.append(uni.name)

                        for person in uni.emails:
                            person_dict = {
                                "University": uni.name,
                                "Keyword": keyword,
                                "Name": person.name,
                                "Position": person.position,
                                "Email": person.email
                            }
                            uni_emails_df.append(person_dict)

                    # Get hospital emails
                    for person in hos.emails:
                        person_dict = {
                            "University": uni.name,
                            "Hospital": hos.name,
                            "Keyword": keyword,
                            "Name": person.name,
                            "Position": person.position,
                            "Email": person.email
                        }
                        hos_emails_df.append(person_dict)

                    hos_link_dict = {
                        "Hospital": hos.name,
                        "Homepage": hos.homepage,
                        "Keyword": keyword
                    }
                    hos_links_df.append(hos_link_dict)

        # Save university emails
        save_to_xlsx(uni_emails_df, f"{keyword}_Uni_emails")
        # Save hospital emails
        save_to_xlsx(hos_emails_df, f"{keyword}_Hos_emails")
        # Save hospital links
        save_to_csv(hos_links_df, f"{keyword}_Hos_links")


def save_to_csv(data, const):
    df = pd.DataFrame(data)
    csv_file_path = f'{const}_output.csv'
    df.to_csv(csv_file_path, index=False)
    print(f'Data has been saved to {csv_file_path}')


def save_to_xlsx(data, const):
    df = pd.DataFrame(data)
    xlsx_file_path = f'{const}_output.xlsx'
    df.to_csv(xlsx_file_path, index=False)
    print(f'Data has been saved to {xlsx_file_path}')


async def main():
    uni_links = home_page_scrapper(os.getenv('URL'))
    universities = [await get_uni_info(link) for link in uni_links]
    return universities


info = asyncio.run(main())

keywords = os.getenv('KEYWORDS').split(',')
fach_search(info, keywords)

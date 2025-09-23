import json
from bs4.element import Tag
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

def login(page, melody_url, user_id, password):
    print("Logging in...")
    page.goto(melody_url)
    page.fill("#username", user_id)
    page.fill("#password", password)
    page.press("#password", "Enter")
    page.wait_for_timeout(3000)
    print("Logged in successfully.")

def goto_variable_page(page, melody_variables_url):
    temp = {}
    page.goto(melody_variables_url)
    page.wait_for_timeout(2000)

    # Parse with BeautifulSoup
    html_content = page.content()
    soup = BeautifulSoup(html_content, "html.parser")

    pagination_tag = soup.find('ul', class_='pagination')
    pages = 0
    li_tags = []
    if isinstance(pagination_tag, Tag):
        li_tags = pagination_tag.find_all('li')
    if li_tags:
        for li in li_tags:
            text = li.get_text(strip=True)
            if text.isdigit():
                pages = int(text)

    for i in range(0, pages):
        print(f'Processing page {i+1} of {pages}...')
        temp_melody_variables_url = f"https://melody.trendgully.com/variable/list/?page_VariableModelView={str(i)}"

        page.goto(temp_melody_variables_url)
        page.wait_for_timeout(2000)

        # Parse with BeautifulSoup
        html_content = page.content()
        soup = BeautifulSoup(html_content, "html.parser")

        td_tags = soup.find_all('td')
        for i in range(len(td_tags)):
            td_string = td_tags[i].get_text()
            if td_string and '_execution_date' in td_string:
                date_string = td_tags[i + 1].get_text()
                date_split = date_string.split('_')
                date_string = f'{date_split[-1]}-{date_split[1]}-{date_split[0]}'
                brand = td_string.replace('_execution_date', '')
                if brand not in ['andamen']:
                    temp[brand] = date_string
                    print(f'{td_string} : {date_string}')
    return temp

if __name__ == "__main__":
    melody_url = 'https://melody.trendgully.com/home'
    melody_variables_url = 'https://melody.trendgully.com/variable/list/'

    user_id = "intern1.trendgully@gmail.com"
    password = "techspek190"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        

        login(page, melody_url, user_id, password)
        dates = goto_variable_page(page, melody_variables_url)

        with open('pipeline_dates.json', 'w') as f:
            json.dump(dates, f, indent=4)

        print('Pipeline date file created and saved to: pipeline_dates.json')

        browser.close()
import traceback
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
import time
import pandas as pd
import pypyodbc
from mysql.connector import Error
from selenium.common import NoSuchElementException
import schedule
from selenium.webdriver.chrome.service import Service

def connect_db():
    cnxn = None
    try:
        driver = "{ODBC Driver 17 for SQL Server}"
        server = "108.181.198.214"
        database = "Web_Scrapping"
        username = "Sa1"
        password = "Sas@123"

        connection_string = (
            f'DRIVER={driver};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password}'
        )

        cnxn = pypyodbc.connect(connection_string)

        if cnxn:
            print("SQL Server Database connection successful.")
        else:
            print("Failed to connect to SQL Server database.")

    except Exception as err:
        print(f"Error: '{err}'")

    return cnxn


def close_db_connection(cnxn):
    try:
        cnxn.close()
        print('Connection Closed')
    except Error as err:
        print(f"Error: '{err}'")


def scrapping1(cnxn):
    driver = webdriver.Chrome()
    # Navigate to the URL
    driver.get('https://www.cybage.com/careers/open-positions#open-positions')

    #Loading all the jobs in this loop
    while True:

        try:
            # Alternatively, you can scroll to a specific element
            # Find the element you want to scroll to
            element = driver.find_element(By.XPATH, '//a[contains(text(),"Load more")]')

            # Calculate the element's position relative to the viewport
            element_location_y = element.location['y']
            element_height = element.size['height']
            viewport_height = driver.execute_script("return window.innerHeight;")
            scroll_amount = element_location_y - (viewport_height / 2) + (element_height / 2)+20

            # Scroll the element into the center of the viewport
            driver.execute_script("window.scrollTo(0, arguments[0]);", scroll_amount)
            time.sleep(5)

            # add the load more button xpath here
            driver.find_element('xpath','//a[contains(text(),"Load more")]').click()
            time.sleep(5)

        except:
            break


    jobs = driver.find_elements(By.XPATH, '//*[@id="open-positions"]/div/div/div[2]/div/table[2]/tbody/tr/td[1]/a')

    j = 1
    for item in jobs:
        joblist1 = []
        job = {}

        job_url = item.get_attribute('href')

        # Open a new tab
        driver.execute_script("window.open('about:blank', 'new_tab')")

        # Switch to the new tab
        new_tab_handle = driver.window_handles[-1]
        driver.switch_to.window(new_tab_handle)

        # Open the desired URL in the new tab
        driver.get(job_url)
        time.sleep(20)

        try:
            job['JobTitle'] = driver.find_element("xpath",'//span[@class="field field--name-title field--type-string field--label-hidden"]').text.strip()
        except NoSuchElementException as e:
            print(e)
            job['JobTitle'] = ''

        try:
            job['Overview'] = '“We, the visionaries are on a relentless mission to transform the world of product engineering by combining our unwavering leadership ethos, data-driven approach, and unyielding focus on technology. We believe in creating an extraordinary culture that fosters equality and excellence, ensuring a methodical, consistent, and unparalleled delivery experience for our customers”.'
        except NoSuchElementException as e:
            print(e)
            job['Overview'] = ''

        try:
            if j == 1:
                job['State'] = driver.find_element("xpath",'''//h4[@class="job_info__heading"][contains(text(),'Location')]/following-sibling::*''').text.strip()
            else:
                job['State'] = "India"
        except NoSuchElementException as e:
            print(e)
            job['State'] = ''

        try:
            job['CompanyName'] = 'Cybage'
        except NoSuchElementException as e:
            print(e)
            job['CompanyName'] = ''

        try:
            if j == 1:
                job['JobDescription'] = driver.find_element('xpath','''//div[@class="container"]/div[contains(@class,'about-position')]''').text.strip()
            else:
                job['JobDescription'] = driver.find_element('xpath','''//div[@class="container"]/div[contains(@class,'about-position')]''').text.strip()
        except NoSuchElementException as e:
            print(e)
            job['JobDescription'] = ''

        try:
            if j == 1:
                job['MinimumExperience'] = driver.find_element('xpath','''//h4[contains(text(),'Work Experience')]/following-sibling::div''').text.strip()
            else:
                job['MinimumExperience'] = "0-5 yrs"

        except NoSuchElementException as e:
            print(e)
            job['MinimumExperience'] = ''

        try:
            job['Name'] = '.'
        except NoSuchElementException as e:
            print(e)
            job['Name'] = ''

        try:
            job['Email'] = 'business@cybage.com'
        except NoSuchElementException as e:
            print(e)
            job['Email'] = ''

        try:
            job['ContactNo'] = '+91-20-66041700'
        except NoSuchElementException as e:
            print(e)
            job['ContactNo'] = ''

        try:
            job['JobLink'] = driver.current_url
        except NoSuchElementException as e:
            print(e)
            job['JobLink'] = ''

        try:
            job['CountryName'] = 'India'
        except NoSuchElementException as e:
            print(e)
            job['CountryName'] = ''

        try:
            if j == 1:
                job['MSkills'] = driver.find_element('xpath','''//div[@class="container"]/div[contains(@class,'about-position')]''').text.strip()
            else:
                job['MSkills'] = 'Fluent in english.'
        except NoSuchElementException as e:
            print(e)
            job['MSkills'] = ''

        if any(value == '' or value == 'N/A' for value in
               (job['CountryName'], job['Overview'], job['CompanyName'], job['JobTitle'], job['MSkills'],
                job['JobDescription'], job['JobLink'], job['ContactNo'], job['Email'],
                job['Name'], job['State'], job['MinimumExperience'])):

            print('###########################################')
            print('#   Skipped this job due to NULL values   #')
            print('###########################################')
        else:
            joblist1.append(job)
            data = pd.DataFrame(joblist1)
            columns = ['CountryName', 'CompanyName', 'JobTitle', 'Overview', 'MSkills', 'JobDescription', 'JobLink',
                       'ContactNo',
                       'Email', 'Name', 'State', 'MinimumExperience']
            specified_df = data[columns]
            records = specified_df.values.tolist()
            sql_insert = ''' INSERT INTO WebJobList(CountryName,CompanyName,JobTitle,Overview,MSkills,JobDescription,
                                             JobLink,ContactNo,Email,Name,State,MinimumExperience,DateCreated)
                                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,GETDATE())'''

            print(job)
            print('###########################################')
            print('#           INSERTED SUCCESSFULLY         #')
            print('###########################################')

        # Close the current tab
        driver.close()
        # Switch back to the previous tab
        main_tab_handle = driver.window_handles[0]
        driver.switch_to.window(main_tab_handle)

        time.sleep(1)

    # Close the WebDriver
    driver.quit()
    print('Task is completed')

cnxn = connect_db()

scrapping1(cnxn)
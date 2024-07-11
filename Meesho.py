import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import pypyodbc
from selenium.common.exceptions import NoSuchElementException

# Function to connect to the SQL Server database
def connect_db():
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
        print("SQL Server Database connection successful.")
        return cnxn
    except Exception as err:
        print(f"Error: '{err}'")
        return None

# Function to close the database connection
def close_db_connection(cnxn):
    try:
        cnxn.close()
        print('Connection Closed')
    except Exception as err:
        print(f"Error: '{err}'")

# Function to scrape job listings and insert into the database
def scrapping1(cnxn):
    driver = webdriver.Chrome()
    driver.get('https://www.meesho.io/jobs')

    # Load all jobs by clicking 'Load more' button
    while True:
        try:
            element = driver.find_element(By.XPATH, "//button[contains(@class, 'rounded-full') and contains(@class, 'bg-green-600') and normalize-space(text())='Load more']")
            element_location_y = element.location['y']
            element_height = element.size['height']
            viewport_height = driver.execute_script("return window.innerHeight;")
            scroll_amount = element_location_y - (viewport_height / 2) + (element_height / 2) + 20
            driver.execute_script("window.scrollTo(0, arguments[0]);", scroll_amount)
            time.sleep(5)
            element.click()
            time.sleep(5)
        except NoSuchElementException:
            break

    jobs = driver.find_elements(By.XPATH, '//*[@id="__next"]/div[1]/main/section/div[2]/section[3]/div[1]/div/div/a')

    for item in jobs:
        joblist1 = []
        job = {}
        job_url = item.get_attribute('href')
        driver.execute_script("window.open('about:blank', 'new_tab')")
        new_tab_handle = driver.window_handles[-1]
        driver.switch_to.window(new_tab_handle)
        driver.get(job_url)
        time.sleep(20)

        try:
            job['JobTitle'] = driver.find_element(By.XPATH, '//*[@id="__next"]/div[1]/main/section/div[2]/section/div/div/h1').text.strip()
        except NoSuchElementException:
            job['JobTitle'] = ''

        job['Overview'] = '“We, the visionaries are on a relentless mission to transform the world of product engineering by combining our unwavering leadership ethos, data-driven approach, and unyielding focus on technology. We believe in creating an extraordinary culture that fosters equality and excellence, ensuring a methodical, consistent, and unparalleled delivery experience for our customers”.'
        job['State'] = 'Bangalore, Karnataka'
        job['CompanyName'] = 'Meesho'
        try:
            job['JobDescription'] = driver.find_element(By.XPATH, '//*[@id="__next"]/div[1]/main/section/div[2]/div/div/section').text.strip()
        except NoSuchElementException:
            job['JobDescription'] = ''
        job['MinimumExperience'] = 'NA'
        job['Name'] = 'NA'
        job['Email'] = 'hr@meesho.com'
        job['ContactNo'] = '+91 89710 18888'
        job['JobLink'] = driver.current_url
        job['CountryName'] = 'India'
        job['MSkills'] = 'NA'

        print(f"Scraped Job: {job}")

        # Only skip if critical fields are empty or 'NA'
        critical_fields = ['JobTitle', 'JobDescription', 'CompanyName', 'JobLink']
        if not any(job[field] in ('', 'NA') for field in critical_fields):
            joblist1.append(job)
            data = pd.DataFrame(joblist1)
            columns = ['CountryName', 'CompanyName', 'JobTitle', 'Overview', 'MSkills', 'JobDescription', 'JobLink', 'ContactNo', 'Email', 'Name', 'State', 'MinimumExperience']
            specified_df = data[columns]
            records = specified_df.values.tolist()
            sql_insert = '''INSERT INTO WebJobList (CountryName, CompanyName, JobTitle, Overview, MSkills, JobDescription, JobLink, ContactNo, Email, Name, State, MinimumExperience, DateCreated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())'''

            cursor = cnxn.cursor()
            try:
                print("Inserting into database...")
                cursor.executemany(sql_insert, records)
                cnxn.commit()
                print('###########################################')
                print('#           INSERTED SUCCESSFULLY         #')
                print('###########################################')
            except Exception as e:
                print(f"Error inserting job: {job['JobTitle']} - {e}")
                cnxn.rollback()
            finally:
                cursor.close()
        else:
            print('###########################################')
            print('#   Skipped this job due to NULL values   #')
            print('###########################################')

        driver.close()
        main_tab_handle = driver.window_handles[0]
        driver.switch_to.window(main_tab_handle)
        time.sleep(1)

    driver.quit()
    print('Task is completed')

# Connect to the database
cnxn = connect_db()

if cnxn:
    scrapping1(cnxn)
    close_db_connection(cnxn)
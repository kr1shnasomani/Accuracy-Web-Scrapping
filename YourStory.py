from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import pypyodbc
from mysql.connector import Error
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

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
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get('https://yourstory.com/jobs')
    time.sleep(5)  # Ensure the page is fully loaded

    jobs = driver.find_elements(By.XPATH, '/html/body/section[2]/div[2]/a')
    print(f"Found {len(jobs)} jobs on the page.")

    for i, item in enumerate(jobs):
        joblist1 = []
        job = {}
        job_url = item.get_attribute('href')
        print(f"Processing job URL: {job_url}")

        # Open a new tab
        driver.execute_script("window.open('about:blank', 'new_tab')")
        new_tab_handle = driver.window_handles[-1]
        driver.switch_to.window(new_tab_handle)
        driver.get(job_url)
        time.sleep(5)

        try:
            job['JobTitle'] = driver.find_element(By.XPATH, '/html/body/section[1]/div/div/h1').text.strip()
        except NoSuchElementException as e:
            print(f"Error finding JobTitle: {e}")
            job['JobTitle'] = ''

        job['Overview'] = """YourStory, India’s leading platform for positive change, believes in the transformative power of storytelling. For over fifteen years, it has highlighted the journeys of countless changemakers, including startups that have grown into billion-dollar companies, and unsung entrepreneurs contributing to a better India. Committed to inclusivity, YourStory shares diverse voices to ensure everyone has equal opportunities to succeed."""
        job['State'] = 'Bangalore'
        job['CompanyName'] = 'YourStory'

        try:
            job['JobDescription'] = driver.find_element(By.XPATH, '/html/body/section[2]/div/div').text.strip()
        except NoSuchElementException as e:
            print(f"Error finding JobDescription: {e}")
            job['JobDescription'] = ''

        job['MinimumExperience'] = 'NA'
        job['Name'] = 'NA'
        job['Email'] = 'hr@yourstory.com'
        job['ContactNo'] = 'NA'
        job['JobLink'] = driver.current_url
        job['CountryName'] = 'India'

        try:
            job['MSkills'] = 'NA'
        except NoSuchElementException as e:
            print(f"Error finding MSkills: {e}")
            job['MSkills'] = ''

        if any(value == '' for value in
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
                       'ContactNo', 'Email', 'Name', 'State', 'MinimumExperience']
            specified_df = data[columns]
            records = specified_df.values.tolist()
            sql_insert = ''' INSERT INTO WebJobList(CountryName,CompanyName,JobTitle,Overview,MSkills,JobDescription,
                                             JobLink,ContactNo,Email,Name,State,MinimumExperience,DateCreated)
                                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())'''
            try:
                cursor = cnxn.cursor()
                cursor.executemany(sql_insert, records)
                cursor.commit()
                print('###########################################')
                print('#           INSERTED SUCCESSFULLY         #')
                print('###########################################')
            except Exception as e:
                cursor.rollback()
                print(f"Database insertion error: {e}")
            finally:
                cursor.close()

        # Close the current tab and switch back to the main tab
        driver.close()
        main_tab_handle = driver.window_handles[0]
        driver.switch_to.window(main_tab_handle)

        time.sleep(1)

    driver.quit()
    print('Task is completed')

cnxn = connect_db()

if cnxn:
    scrapping1(cnxn)
    close_db_connection(cnxn)
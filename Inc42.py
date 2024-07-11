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
    driver.get('https://careers.inc42.com/jobs')
    time.sleep(5)  # Ensure the page is fully loaded

    jobs = driver.find_elements(By.XPATH, '/html/body/div/div[2]/div[1]/div/ul/li/div[2]/div/a[1]')
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
            job['JobTitle'] = driver.find_element(By.XPATH, '//*[@id="job-details-header"]/div/div/div[1]/h1').text.strip()
        except NoSuchElementException as e:
            print(f"Error finding JobTitle: {e}")
            job['JobTitle'] = ''

        job['Overview'] = """Inc42 is India’s largest tech media platform, dedicated to accelerating the GDP of India’s tech and startup economy. The platform is passionate about innovation, entrepreneurship, and the remarkable journey of startups in India. For over a decade, it has bolstered India’s tech and startup economy by focusing on three core elements: content, people, and connections."""
        job['State'] = 'New Delhi'
        job['CompanyName'] = 'Inc42'

        try:
            job['JobDescription'] = driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]').text.strip()
        except NoSuchElementException as e:
            print(f"Error finding JobDescription: {e}")
            job['JobDescription'] = ''

        job['MinimumExperience'] = 'NA'
        job['Name'] = 'NA'
        job['Email'] = 'grievances@inc42.com'
        job['ContactNo'] = '011 4036 3558'
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
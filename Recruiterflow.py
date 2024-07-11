from selenium import webdriver
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
    driver.get('https://recruiterflow.com/rfcareers/jobs')

    # More Details button
    jobs = driver.find_elements(By.XPATH, '//*[@id="rf-jobs-list"]/div/div/span/a')

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

        time.sleep(4)

        try:
            # job['JobTitle] = Software Engineer
            job['JobTitle'] = driver.find_element("xpath",'//*[@id="react-root"]/div/div[1]/div[2]/div/div/div/div[1]/div[1]/div/div[1]').text.strip()
        except NoSuchElementException as e:
            print(e)
            job['JobTitle'] = ''

        try:
            job['Overview'] = """Recruiterflow is a recruitment software designed for recruiting and staffing agencies. It offers an Applicant Tracking System (ATS) and Customer Relationship Management (CRM) to streamline the hiring process. Key features include job posting, candidate sourcing through a Chrome extension, email automation, and a client portal. Recruiterflow also emphasizes data ownership, allowing users to export their data via API."""
        except NoSuchElementException as e:
            print(e)
            job['Overview'] = ''

        try:
            # job['State'] = Hyderabad
            job['State'] = driver.find_element("xpath",'//*[@id="react-root"]/div/div[1]/div[2]/div/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div').text.strip()
        except NoSuchElementException as e:
            print(e)
            job['State'] = ''

        try:
            job['CompanyName'] = 'Recruiterflow'
        except NoSuchElementException as e:
            print(e)
            job['CompanyName'] = ''

        try:
            # Description of the job
            if j == 1:
             job['JobDescription'] = driver.find_element("xpath",'//*[@id="react-root"]/div/div[1]/div[3]').text.strip()
            else:
                job['JobDescription'] = driver.find_element("xpath",'//*[@id="react-root"]/div/div[1]/div[3]').text.strip()
        except NoSuchElementException as e:
            print(e)
            job['JobDescription'] = ''

        try:
            # job['MinimumExperience'] = 6-10 yrs
            job['MinimumExperience'] = 'NA'
        except NoSuchElementException as e:
            print(e)
            job['MinimumExperience'] = ''

        try:
            job['Name'] = 'NA'
        except NoSuchElementException as e:
            print(e)
            job['Name'] = ''

        try:
            job['Email'] = 'NA'
        except NoSuchElementException as e:
            print(e)
            job['Email'] = ''

        try:
            job['ContactNo'] = 'NA'
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
            job['MSkills'] = 'NA'
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
            try:
                cursor = cnxn.cursor()
                cursor.executemany(sql_insert, records)
                cursor.commit();
            except Exception as e:
                cursor.rollback()
                print(str(e[1]))
            finally:
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
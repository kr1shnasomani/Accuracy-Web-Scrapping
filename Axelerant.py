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
    driver.get('https://www.axelerant.com/careers/open-positions')
    time.sleep(20)
    driver.find_element('xpath','//*[@id="hs-eu-confirmation-button"]').click()
    time.sleep(10)

    # More Details button
    jobs = driver.find_elements(By.XPATH, '//*[@id="axl_job_posts"]/div/ul/li/div/div/a[2]')

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
            job['JobTitle'] = driver.find_element("xpath",'//*[@id="job_title"]').text.strip()
        except NoSuchElementException as e:
            print(e)
            job['JobTitle'] = ''

        try:
            job['Overview'] = """Axelerant assists digital changemakers in driving transformation. Their clients range from agencies and enterprises to diverse organizations. Axelerant’s comprehensive services encompass Drupal, Acquia, DevOps, Frontend, Quality Engineering, and Consulting."""
        except NoSuchElementException as e:
            print(e)
            job['Overview'] = ''

        try:
            # job['State'] = Hyderabad
            job['State'] = 'India Remote'
        except NoSuchElementException as e:
            print(e)
            job['State'] = ''

        try:
            job['CompanyName'] = 'Axelerant'
        except NoSuchElementException as e:
            print(e)
            job['CompanyName'] = ''

        try:
            # Description of the job
            if j == 1:
             job['JobDescription'] = driver.find_element("xpath",'//*[@id="key_res"]').text.strip()
            else:
                job['JobDescription'] = driver.find_element("xpath",'//*[@id="key_res"]').text.strip()
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
            job['Email'] = 'hello@axelerant.com'
        except NoSuchElementException as e:
            print(e)
            job['Email'] = ''

        try:
            job['ContactNo'] = '+1 678 935 7435'
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
             job['MSkills'] = driver.find_element("xpath",'//*[@id="skills"]/div/ul[1]').text.strip()
            else:
                job['MSkills'] = driver.find_element("xpath",'//*[@id="skills"]/div/ul[1]').text.strip()
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



        # used to go back
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
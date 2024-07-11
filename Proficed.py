from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pypyodbc
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime

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
    except Exception as err:
        print(f"Error: '{err}'")


def scrapping1(cnxn):
    try:
        driver = webdriver.Chrome()
        driver.get('https://proficed.com/jobs')
        time.sleep(20)
        driver.find_element('xpath',"//a[text()='I Agree']").click()
        time.sleep(10)
        jobs = driver.find_elements(By.XPATH, '/html/body/div[1]/div/section[2]/div/div/div/div/div/h4/a')

        joblist1 = []

        for j, item in enumerate(jobs, start=1):
            job = {}

            job_url = item.get_attribute('href')

            driver.execute_script("window.open('about:blank', 'new_tab')")
            new_tab_handle = driver.window_handles[-1]
            driver.switch_to.window(new_tab_handle)
            driver.get(job_url)

            time.sleep(4)

            try:
                job['JobTitle'] = driver.find_element(By.XPATH, '//*[@class="col-md-8 order-2 order-md-1 align-self-center p-static"]/h1').text.strip()
            except NoSuchElementException as e:
                print(e)
                job['JobTitle'] = 'Not specified'

            job['Overview'] = "Founded in 2011 by Ruchir Gupta, Proficed has built a strong reputation for excellence over its 12-year journey..."

            try:
                job['State'] = driver.find_element(By.XPATH, '//*[@class="card-body bg-color-grey"]/ul[2]/li[1]').text.strip()
            except NoSuchElementException as e:
                print(e)
                job['State'] = 'Not specified'

            job['CompanyName'] = 'Proficed'

            try:
                job['JobDescription'] = driver.find_element(By.XPATH, '//*[@id="roles-and-responsibilities"]').text.strip()
            except NoSuchElementException as e:
                print(e)
                job['JobDescription'] = 'Not specified'

            try:
                job['MinimumExperience'] = driver.find_element(By.XPATH, '/html/body/div[1]/div/section[2]/div/div/div[1]/div[1]/div/ul[1]/li[4]').text.strip()
            except NoSuchElementException as e:
                print(e)
                job['MinimumExperience'] = 'Not specified'

            job['Name'] = 'NA'
            job['Email'] = 'hr@proficed.com'
            job['ContactNo'] = 'NA'
            job['JobLink'] = driver.current_url
            job['CountryName'] = 'India'

            try:
                job['MSkills'] = 'NA'
            except NoSuchElementException as e:
                print(e)
                job['MSkills'] = 'Not specified'

            # Insert job only if critical fields are present
            if job['JobTitle'] != 'Not specified' and job['JobDescription'] != 'Not specified':
                joblist1.append(job)

                try:
                    cursor = cnxn.cursor()
                    sql_insert = '''INSERT INTO WebJobList (CountryName, CompanyName, JobTitle, Overview, MSkills,
                                                             JobDescription, JobLink, ContactNo, Email, Name,
                                                             State, MinimumExperience, DateCreated)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

                    records = [(job['CountryName'], job['CompanyName'], job['JobTitle'], job['Overview'], job['MSkills'],
                                job['JobDescription'], job['JobLink'], job['ContactNo'], job['Email'],
                                job['Name'], job['State'], job['MinimumExperience'], datetime.now())]

                    cursor.executemany(sql_insert, records)
                    cnxn.commit()
                    print(job)
                    print('###########################################')
                    print('#           INSERTED SUCCESSFULLY         #')
                    print('###########################################')
                except Exception as e:
                    cnxn.rollback()
                    print(f"Error: {str(e)}")
            else:
                print(job)
                print('###########################################')
                print('#   Skipped this job due to NULL values   #')
                print('###########################################')

            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        driver.quit()
        print('Task is completed')

# Connect to database
cnxn = connect_db()

# Start scraping
if cnxn:
    scrapping1(cnxn)

# Close database connection
close_db_connection(cnxn)
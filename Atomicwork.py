from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import pypyodbc
from selenium.common.exceptions import NoSuchElementException, WebDriverException


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
        print(f"Error connecting to SQL Server: {err}")
        return None


def close_db_connection(cnxn):
    try:
        cnxn.close()
        print('Database connection closed.')
    except Exception as err:
        print(f"Error closing SQL Server connection: {err}")


def scrapping1(cnxn):
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    service = Service()

    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except WebDriverException as e:
        print(f"Error initializing WebDriver: {e}")
        return

    try:
        driver.get('https://careers.atomicwork.com')

        # Debug print the page source or a snapshot of it
        print(driver.page_source[:1000])  # print the first 1000 characters of the page source

        # Update the XPath to correctly locate the anchor tags with URLs
        job_links = driver.find_elements(By.XPATH, "//a[contains(@class, 'job-list-item')]")

        if not job_links:
            print("No job links found.")
            return

        for link in job_links:
            job_url = link.get_attribute('href')

            if not job_url:
                print("Invalid job URL, skipping...")
                continue

            job = {}

            # Open a new tab
            driver.execute_script("window.open('about:blank', 'new_tab')")
            driver.switch_to.window(driver.window_handles[-1])
            driver.get(job_url)

            time.sleep(4)

            # Extract job details
            try:
                job['JobTitle'] = driver.find_element(By.XPATH, "//h1[contains(@class, 'title')]").text.strip()
            except NoSuchElementException:
                job['JobTitle'] = ''

            job[
                'Overview'] = """Founded by Vijay Rayapati, a former executive at Nutanix, alongside Kiran Darisi and Parsuram Vijayasankar, founding members of Freshworks, Atomicwork is set to transform employee service through conversational ITSM. By uniting people, processes, and platforms, Atomicwork empower organizations to deliver impactful internal support and streamline business operations. With a team of seasoned professionals renowned for their success in enterprise SaaS, Atomicwork is poised to create a world-class company that will revolutionize the lives of their employees, customers, partners, investors, and the entire EX community."""

            try:
                job['State'] = driver.find_element(By.XPATH,
                                                   '/html/body/div[1]/div[2]/div/main/div[1]/ul/li[3]').text.strip()
            except NoSuchElementException:
                job['State'] = ''

            job['CompanyName'] = 'Atomicwork'

            try:
                job['JobDescription'] = driver.find_element(By.XPATH,
                                                            '/html/body/div[1]/div[2]/div/main/div[2]/div/section[1]').text.strip()
            except NoSuchElementException:
                job['JobDescription'] = ''

            try:
                job['MinimumExperience'] = driver.find_element(By.XPATH,
                                                               "/html/body/div[1]/div[2]/div/main/div[2]/div/section[1]/ul[2]/li[1]/p").text.strip()
            except NoSuchElementException:
                job['MinimumExperience'] = ''

            job['Name'] = 'NA'
            job['Email'] = 'sales@atomicwork.com'
            job['ContactNo'] = '+1 (415) 980-5116'
            job['JobLink'] = driver.current_url
            job['CountryName'] = 'India'

            try:
                job['MSkills'] = driver.find_element(By.XPATH,
                                                     '/html/body/div[1]/div[2]/div/main/div[2]/div/section[1]/ul[2]').text.strip()
            except NoSuchElementException:
                job['MSkills'] = ''

            # Check if all necessary fields are present for insertion
            if all(job.values()):
                try:
                    cursor = cnxn.cursor()

                    # Prepare data for insertion
                    data = pd.DataFrame([job])
                    columns = ['CountryName', 'CompanyName', 'JobTitle', 'Overview', 'MSkills', 'JobDescription',
                               'JobLink',
                               'ContactNo', 'Email', 'Name', 'State', 'MinimumExperience']
                    specified_df = data[columns]
                    records = specified_df.values.tolist()

                    # SQL query for insertion
                    sql_insert = ''' 
                    INSERT INTO WebJobList
                    (CountryName, CompanyName, JobTitle, Overview, MSkills, JobDescription, JobLink,
                    ContactNo, Email, Name, State, MinimumExperience, DateCreated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())'''

                    cursor.executemany(sql_insert, records)
                    cnxn.commit()
                    print('###########################################')
                    print('#           INSERTED SUCCESSFULLY         #')
                    print('###########################################')

                except Exception as e:
                    cnxn.rollback()
                    print(f"Database insert error: {e}")

                finally:
                    cursor.close()

            else:
                print('###########################################')
                print('#   Skipped this job due to NULL values   #')
                print('###########################################')

            # Close the current tab and switch back
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(1)

    except Exception as e:
        print(f"Scraping error: {e}")

    finally:
        driver.quit()
        print('Task is completed')

    close_db_connection(cnxn)


# Connect to the database
cnxn = connect_db()

# Start scraping jobs and inserting into the database
if cnxn:
    scrapping1(cnxn)
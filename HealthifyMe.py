import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import pypyodbc
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

        cnxn = pypyodbc.connect(connection_string, timeout=5)

        if cnxn:
            print("SQL Server Database connection successful.")
        else:
            print("Failed to connect to SQL Server database.")

    except pypyodbc.DatabaseError as db_err:
        print(f"Database error: {db_err}")
    except Exception as err:
        print(f"Error: {traceback.format_exc()}")

    return cnxn

def scroll_to_middle(element, driver):
    element_location_y = element.location['y']
    element_height = element.size['height']
    viewport_height = driver.execute_script("return window.innerHeight;")
    scroll_amount = element_location_y - (viewport_height / 2) + (element_height / 2) - 50

    driver.execute_script("window.scrollTo(0, arguments[0]);", scroll_amount)

def close_db_connection(cnxn):
    try:
        cnxn.close()
        print('Connection Closed')
    except pypyodbc.Error as err:
        print(f"Error: '{err}'")

def scrapping1(cnxn):
    if cnxn is None:
        print("No database connection. Skipping scraping.")
        return

    driver = webdriver.Chrome()
    driver.maximize_window()
    try:
        driver.get('https://healthify.darwinbox.in/ms/candidate/careers')
        time.sleep(2)

        while True:
            jobs = driver.find_elements(By.XPATH, '/html/body/app-root/div/app-user-views/div[2]/app-jobs-wrapper/app-jobs/div/div[3]/div/div[3]/app-jobs-list/app-custom-table/table/tbody/tr/td/a')

            for job_number, item in enumerate(jobs):
                joblist1 = []
                job = {}
                time.sleep(1)

                try:
                    driver.execute_script("window.open(arguments[0], '_blank');", item.get_attribute('href'))
                    time.sleep(2)
                    new_tab_handle = driver.window_handles[-1]
                    driver.switch_to.window(new_tab_handle)
                except:
                    print("Error opening new tab.")
                    continue

                time.sleep(4)

                try:
                    job['JobTitle'] = driver.find_element(By.XPATH, '/html/body/app-root/div/app-user-views/div[2]/app-jobs-wrapper/app-job-details/div/div[1]/div[1]/div[1]/div/div/h4').text.strip()
                except NoSuchElementException:
                    job['JobTitle'] = ''

                job['Overview'] = """HealthifyMe is a comprehensive health and wellness platform founded in 2012, offering personalized solutions for fitness, nutrition, and weight management. Leveraging AI and a vast database, the app provides tailored diet plans, workout routines, and health tracking. Users can consult certified nutritionists and fitness coaches for real-time guidance. With features like calorie tracking, water intake monitoring, and activity tracking, HealthifyMe supports holistic health improvement. Available on Android and iOS, it caters to millions seeking to achieve their fitness goals, manage weight, and adopt healthier lifestyles through scientifically-backed and user-friendly tools."""

                try:
                    job['State'] = driver.find_element(By.XPATH, '/html/body/app-root/div/app-user-views/div[2]/app-jobs-wrapper/app-jobs/div/div[3]/div/div[3]/app-jobs-list/app-custom-table/table/tbody/tr/td[3]/span').text.strip().replace(', India', '')
                except NoSuchElementException:
                    job['State'] = ""

                job['CompanyName'] = 'HealthifyMe'

                try:
                    job['JobDescription'] = driver.find_element(By.XPATH, '/html/body/app-root/div/app-user-views/div[2]/app-jobs-wrapper/app-job-details/div/div[2]/div[1]/div').text.strip()
                except NoSuchElementException:
                    job['JobDescription'] = ''

                job['Name'] = '-'
                job['MinimumExperience'] = '-'
                job['Email'] = 'support@healthifyme.com'
                job['ContactNo'] = '1800 419 9501'
                job['JobLink'] = driver.current_url
                job['CountryName'] = 'India'
                job['MSkills'] = '-'

                if any(value == 'N/A' for value in job.values()):
                    print('###########################################')
                    print('#  Skipped this job due to NULL values   #')
                    print('###########################################')
                else:
                    joblist1.append(job)
                    data = pd.DataFrame(joblist1)
                    columns = ['CountryName', 'CompanyName', 'JobTitle', 'Overview', 'MSkills', 'JobDescription', 'JobLink',
                               'ContactNo', 'Email', 'Name', 'State', 'MinimumExperience']
                    specified_df = data[columns]
                    records = specified_df.values.tolist()
                    sql_insert = '''INSERT INTO WebJobList (CountryName, CompanyName, JobTitle, Overview, MSkills, JobDescription,
                                                            JobLink, ContactNo, Email, Name, State, MinimumExperience, DateCreated)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())'''
                    try:
                        cursor = cnxn.cursor()
                        cursor.executemany(sql_insert, records)
                        cnxn.commit()
                        print('Records inserted successfully.')
                    except Exception as e:
                        print(f"Error inserting records: {traceback.format_exc()}")
                        cnxn.rollback()
                    finally:
                        cursor.close()

                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(1)

            try:
                elements = driver.find_elements(By.XPATH, "//a[@class='page-link' and normalize-space(text())='›']")
                if elements:
                    element = elements[0]
                    scroll_to_middle(element, driver)
                    element.click()
                    time.sleep(5)
                else:
                    print("Next button not found. Stopping pagination.")
                    break
            except:
                traceback.print_exc()
                break
    except Exception as e:
        print(f"Error during scraping: {traceback.format_exc()}")
    finally:
        driver.quit()
        print('Scraping task completed.')

# Main execution
cnxn = connect_db()
scrapping1(cnxn)
if cnxn:
    close_db_connection(cnxn)
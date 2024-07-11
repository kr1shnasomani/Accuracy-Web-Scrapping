import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import pypyodbc
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

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

        cnxn = pypyodbc.connect(connection_string, timeout=5)
        print("SQL Server Database connection successful.")
        return cnxn
    except pypyodbc.DatabaseError as db_err:
        print(f"Database error: {db_err}")
    except Exception as err:
        print(f"Error: {traceback.format_exc()}")
        return None

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
        driver.get('https://razorpay.com/jobs/jobs-all/')
        time.sleep(2)

        while True:
            jobs = driver.find_elements(By.XPATH, '//*[@id="__next"]/main/div[2]/div/div[4]/a')
            for job_number, item in enumerate(jobs):
                joblist1 = []
                job = {}
                time.sleep(1)

                try:
                    ActionChains(driver).key_down(Keys.CONTROL).click(item).key_up(Keys.CONTROL).perform()
                    time.sleep(2)
                    new_tab_handle = driver.window_handles[-1]
                    driver.switch_to.window(new_tab_handle)
                except Exception as e:
                    print(f"Error opening new tab: {e}")
                    continue

                time.sleep(4)

                try:
                    job['JobTitle'] = driver.find_element(By.XPATH, '//*[@id="__next"]/section/div/div/div/div/div/h5').text.strip()
                except NoSuchElementException:
                    job['JobTitle'] = 'Not Specified'

                job['Overview'] = """Razorpay serves as a comprehensive platform for all online payments and banking requirements for businesses. It supports a wide range of payment modes including credit cards, debit cards, net banking, UPI, and popular wallets like JioMoney, Mobikwik, Airtel Money, FreeCharge, Ola Money, and PayZapp."""

                try:
                    job['State'] = driver.find_element(By.XPATH, '//*[@id="__next"]/section/div/div/div/div/div/div/p').text.strip().replace(', India', '')
                except NoSuchElementException:
                    job['State'] = 'Not Specified'

                job['CompanyName'] = 'Razorpay'

                try:
                    job['JobDescription'] = driver.find_element(By.XPATH, '//*[@id="main"]').text.strip()
                except NoSuchElementException:
                    job['JobDescription'] = 'Not Specified'

                job['MinimumExperience'] = 'Not Specified'
                job['Name'] = 'Not Specified'
                job['Email'] = 'careers@razorpay.com'
                job['ContactNo'] = '+91 80482 60001'
                job['JobLink'] = driver.current_url
                job['CountryName'] = 'India'
                job['MSkills'] = 'Not Specified'

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
                element = driver.find_elements(By.XPATH, '//*[@id="__next"]/main/div[2]/div/div[4]/div/ul/li[9]/div/span/img')[0]
                scroll_to_middle(element, driver)
                time.sleep(1)
                ActionChains(driver).move_to_element(element).click(element).perform()
                time.sleep(5)
            except (NoSuchElementException, ElementClickInterceptedException) as e:
                print(f"Error clicking next page: {traceback.format_exc()}")
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
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
    # Calculate the element's position relative to the viewport
    element_location_y = element.location['y']
    element_height = element.size['height']
    viewport_height = driver.execute_script("return window.innerHeight;")
    scroll_amount = element_location_y - (viewport_height / 2) + (element_height / 2) - 50

    # Scroll the element into the center of the viewport
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
        driver.get('https://jobs.harman.com/en_US/careers/SearchJobs/?198=%5B59985%5D&198_format=869&listFilterMode=1&jobRecordsPerPage=6&')
        time.sleep(2)

        while True:
            jobs = driver.find_elements(By.XPATH, '//article[@class="article article--result"]//h3[@class="article__header__text__title title title--04"]//a')

            for job_number, item in enumerate(jobs):
                joblist1 = []
                job = {}
                time.sleep(1)

                try:
                    # Simulate Ctrl + Click to open the link in a new tab
                    ActionChains(driver).key_down(Keys.CONTROL).click(item).key_up(Keys.CONTROL).perform()
                    time.sleep(2)
                    # Switch to the new tab
                    new_tab_handle = driver.window_handles[1]
                    driver.switch_to.window(new_tab_handle)
                except:
                    print("Error opening new tab.")
                    continue  # Skip to next iteration if tab opening fails

                time.sleep(4)

                # Populate job dictionary with scraped data
                try:
                    job['JobTitle'] = driver.find_element(By.XPATH, '//h2[@class="banner__text__title"]').text.strip()
                except NoSuchElementException:
                    job['JobTitle'] = ''

                job['Overview'] = """Around the world, we’re a multi-disciplinary team that’s relentless in our pursuit of what’s next. Together, we think beyond borders, connect with like-minded colleagues, and create powerful experiences that enhance modern life. Get a glimpse into first-person accounts from the people behind our renowned technologies, solutions, platforms, and products. Hear why the spirit behind #HarmanConnectsMe inspired us to join HARMAN, and what it means to call this award-winning organization home."""

                try:
                    job['State'] = driver.find_element(By.XPATH, '//div[@class="article__content__view__field__label"][contains(normalize-space(), \'Additional Location:\')]/following-sibling::div').text.strip().replace(', India', '')
                except NoSuchElementException:
                    job['State'] = ""

                job['CompanyName'] = 'Harman'

                try:
                    job['JobDescription'] = driver.find_element(By.XPATH, '//article[@class="article article--details description-separator tf_sectionApplyButton"]').text.strip()
                except NoSuchElementException:
                    job['JobDescription'] = ''

                job['MinimumExperience'] = '-'
                job['Name'] = '-'
                job['Email'] = 'HarmanCareers@harman.com'
                job['ContactNo'] = '-'
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

                # Close the current tab and switch back to the main tab
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(1)

            try:
                # Find and click the next button
                element = driver.find_elements(By.XPATH, '//a[@class="list-controls__pagination__item paginationNextLink"]')[0]
                scroll_to_middle(element, driver)
                element.click()
                time.sleep(5)
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
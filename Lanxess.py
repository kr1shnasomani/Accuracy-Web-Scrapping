import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import pypyodbc
from selenium.common.exceptions import NoSuchElementException
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
    scraped_jobs = set()
    
    try:
        driver.get('https://career.lanxess.com/search/?q=&locationsearch=India&searchby=location&d=15')
        time.sleep(2)
        driver.find_element(By.XPATH, '//*[@id="cookie-accept"]').click()
        time.sleep(10)

        while True:
            jobs = driver.find_elements(By.XPATH, '//*[@id="searchresults"]/tbody/tr/td[1]/span')

            for item in jobs:
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

                current_url = driver.current_url
                if current_url in scraped_jobs:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                    continue  # Skip already scraped jobs

                scraped_jobs.add(current_url)

                # Populate job dictionary with scraped data
                try:
                    job['JobTitle'] = driver.find_element(By.XPATH, '//*[@id="content"]/div/div[3]/div/div[1]/div[2]/div[2]/div/div/div/h1/span[2]').text.strip()
                except NoSuchElementException:
                    job['JobTitle'] = ''

                job['Overview'] = """LANXESS AG is a prominent German specialty chemicals company headquartered in Cologne, founded in 2004 from a Bayer AG spin-off. It specializes in the development, manufacturing, and marketing of chemical intermediates, additives, specialty chemicals, and plastics. The company serves various industries, including automotive, construction, and electronics, with a commitment to innovation, sustainability, and environmental responsibility. LANXESS operates globally, emphasizing efficient production and advanced technologies to enhance the performance and sustainability of its products. Notable for its robust product portfolio, LANXESS focuses on creating value through high-quality, reliable chemical solutions."""

                try:
                    job['State'] = driver.find_element(By.XPATH, '//*[@id="searchresults"]/tbody/tr/td[2]/span').text.strip().replace(', India', '')
                except NoSuchElementException:
                    job['State'] = ""

                job['CompanyName'] = 'Lanxess'

                try:
                    job['JobDescription'] = driver.find_element(By.XPATH, '//*[@id="content"]/div/div[3]/div/div[1]/div[2]/div[8]/div/div/div/span/span/div/div[1]/div[2]/ul').text.strip()
                except NoSuchElementException:
                    job['JobDescription'] = ''

                try:
                    job['MinimumExperience'] = driver.find_element(By.XPATH, '//*[@id="content"]/div/div[3]/div/div[1]/div[2]/div[8]/div/div/div/span/span/div/div[2]/div[2]/ul/li[2]/span').text.strip()
                except NoSuchElementException:
                    job['MinimumExperience'] = ''

                job['Name'] = '-'
                job['Email'] = '-'
                job['ContactNo'] = '+91-22-6875 1000'
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
                # Check if the next button is present and clickable
                next_buttons = driver.find_elements(By.XPATH, '//*[@id="content"]/div/div/div/div/ul/li/a/span[text()="Next"]')
                if next_buttons:
                    next_button = next_buttons[-1]  # Select the last 'Next' button if multiple found
                    if 'disabled' not in next_button.get_attribute('class'):
                        scroll_to_middle(next_button, driver)
                        next_button.click()
                        time.sleep(5)
                    else:
                        print('Next button is disabled. No more pages to scrape.')
                        break  # Exit the loop if the next button is disabled
                else:
                    print('Next button not found, likely the last page.')
                    break

            except NoSuchElementException:
                print('Next button not found, likely the last page.')
                break
            except Exception:
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
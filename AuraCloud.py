from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import pypyodbc
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service

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
        print(f"Error connecting to the database: '{err}'")
        return None

def close_db_connection(cnxn):
    try:
        cnxn.close()
        print('Connection Closed')
    except Exception as err:
        print(f"Error closing the connection: '{err}'")

def scrapping1(cnxn):
    driver = webdriver.Chrome()
    try:
        # Navigate to the URL
        driver.get('https://careers.auracloud.com/jobs?split_view=true&geobound_coordinates%5Btop_left_lat%5D=12.923962609229514&geobound_coordinates%5Btop_left_lon%5D=77.51588881015778&geobound_coordinates%5Bbottom_right_lat%5D=12.921086903812554&geobound_coordinates%5Bbottom_right_lon%5D=77.51913964748383&query=')
        time.sleep(20)
        driver.find_element(By.XPATH, "/html/body/dialog[1]/div[2]/button[1]").click()
        time.sleep(10)

        jobs = driver.find_elements(By.XPATH, '//*[@id="jobs_list_container"]/li/a')

        joblist1 = []
        for item in jobs:
            job = {}
            job_url = item.get_attribute('href')

            # Validate job_url
            if not job_url:
                print("Invalid job URL. Skipping this job.")
                continue

            # Open a new tab
            driver.execute_script("window.open('about:blank', 'new_tab')")

            # Switch to the new tab
            new_tab_handle = driver.window_handles[-1]
            driver.switch_to.window(new_tab_handle)

            # Open the desired URL in the new tab
            driver.get(job_url)

            time.sleep(4)

            try:
                job['JobTitle'] = driver.find_element(By.XPATH, '/html/body/main/section[1]/div[1]/div/h1').text.strip()
            except NoSuchElementException:
                job['JobTitle'] = ''

            job['Overview'] = """Aura Cloud empowers financial institutions to be agile, addressing both incumbent and new entrant challenges. Large institutions grapple with cost efficiency, while new players face high customer acquisition costs. Banks seek new revenue streams and collaboration with FinTechs, who aim to ensure credibility without losing focus. Aura Cloud offers strategic support in this disruptive financial landscape. Their platform accelerates service launches and integrates with ecosystem providers, enabling real-time business opportunities. Committed to continuous support, Aura Cloud leverages strong domain expertise and market knowledge, acting as an invisible partner in achieving financial success and agility."""

            job['CompanyName'] = 'Aura Cloud'

            try:
                job['JobDescription'] = driver.find_element(By.XPATH, '/html/body/main/section[2]').text.strip()
            except NoSuchElementException:
                job['JobDescription'] = ''

            try:
                job['MinimumExperience'] = driver.find_element(By.XPATH, '/html/body/main/section[2]/div/ul[1]/li[1]').text.strip()
            except NoSuchElementException:
                job['MinimumExperience'] = ''

            job['ContactNo'] = '+ 46 10 214 62 55'
            job['JobLink'] = driver.current_url

            job['Name'] = '-'
            job['Email'] = '-'
            job['MSkills'] = '-'
            job['State'] = 'Bangalore'
            job['CountryName'] = 'India'

            # Change this condition to fit your needs
            if True:  # Always add the job for now to test the database insertion
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
                    print("Attempting to insert the following job:")
                    print(job)  # Log the job being inserted
                    cursor.executemany(sql_insert, records)
                    cnxn.commit()  # Fix: Commit to the connection, not the cursor
                    print('###########################################')
                    print('#           INSERTED SUCCESSFULLY         #')
                    print('###########################################')
                except Exception as e:
                    cnxn.rollback()
                    print(f"Error inserting into the database: {str(e)}")
                finally:
                    cursor.close()
            
            # Clean up for next iteration
            joblist1.clear()

            # Close the current tab
            driver.close()
            # Switch back to the previous tab
            main_tab_handle = driver.window_handles[0]
            driver.switch_to.window(main_tab_handle)

            time.sleep(1)

    except Exception as e:
        print(f"Error during scraping: {str(e)}")
    finally:
        # Close the WebDriver
        driver.quit()
        print('Task is completed')

# Main execution
cnxn = connect_db()

if cnxn:
    scrapping1(cnxn)
    close_db_connection(cnxn)
else:
    print("No connection to the database.")
# Accuracy-Web-Scrapping

1. Import the following libraries:
   - Selenium
   - pandas

2. Enter the details of the driver, server, database name, username and password

3. Copy the link of the company's career page and paste it in the code.

4. If there is a pop up asking about the cookies, then follow the following steps: left click > select 'Inspect Element' option > choose the 'Agree' option > create a XPath > copy it in the code.

5. Three cases occur up while scrapping job information from websites:
   - More Details Button: Companies which have option like 'More Details' come under this case. For example: https://www.knstek.com/jobs/. Follow the same steps as given on point number 4, but instead of the 'Agree' option choose the 'More Details' option.
   - Load More Button: Companies which have option like 'Load More' come under this case. For example: https://www.cybage.com/careers/open-positions#open-positions. Follow the same steps as given on point number 4, but instead of the 'Agree' option choose the 'Load More' option. Additional to that copy the XPath of the link under the job title.
   -  Next Page Button: Companies which have option like 'Next Page' come under this case. For example: https://myhr.darwinbox.in/ms/candidate/careers. Follow the same steps as given on point number 4, but instead of the 'Agree' option choose the 'Next Page' option. Additional to that copy the XPath of the link under the job title.
  
6. Insert the XPath of the information you want to scrape, such as job title, job location, minimum experience and job description. Also provide an overview of the company in less than 100 words, company's name and the HR contact information. The overview of the company can be taken from the company's 'About Us' section and the company's HR contact information can be taken from the 'Contact Us' section. For the information not avaiable, fill 'NA' or '-' in the space given in the code.

7. Now run the code, if the output says 'INSERTED SUCCESSFULLY' then it means that all the required information from the company's website was inserted into the server. But if the output is 'Skipped this job due to NULL values' then there is some missing information. In this case mostly the issue is with the XPath, so we try providing the correct XPath.

8. During the scrapping task, the website link provided initially will open up on Google Chrome and one by one all the jobs will open on another new tab and close after the information is taken. Once all the jobs are scrapped and entered into the server the Google Chrome window will close itself and there will be a output saying 'Task is completed'.

9. Download SQL Server Management Studio 20 from the internet. After downloading is completed, enter the username, password and server name. Now click on 'New Query' and copy the following lines there:

   use *enter the database name*;
   select * from WebJobList where CompanyName = '*enter the company name*';

10. Once this step is done click on 'Execute'. After this you will be able to see all the scrapped job information in the output section of SQL Server Management Studio 20.

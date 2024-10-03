from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
import csv
import pandas as pd
import time

def get_element_text(driver, by, value):
            """Helper function to get element text or return an empty string if element is not found."""
            try:
                return driver.find_element(by, value).text
            except:
                return ''
            
class CareerViet:
    def __init__(self):
        options = Options()
        #options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-software-rasterizer')  # Important for Mac M1 compatibility
        options.add_argument('--disable-gpu')
        options.add_argument('--remote-debugging-port=9222')
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.stores = []

    def parse(self):
        """ 
        Scrape all pages by navigating to the next page and extracting job listings.
        """
        jobs = []
        
        # Adjust the range as needed for how many pages you want to scrape
        for page_number in range(3, 6):  
            current_url = f"https://careerviet.vn/viec-lam/tieng-trung-tai-ho-chi-minh-kl8-trang-{page_number}-vi.html"
            print(f"Scraping page {page_number}: {current_url}")
            self.driver.get(current_url)
            if self.driver.current_url == 'https://careerviet.vn/viec-lam/tieng-trung-tai-ho-chi-minh-kl8-vi.html':
                print("Skipping the URL as it matches the specified URL.")
                self.driver.get(f"https://careerviet.vn/viec-lam/tieng-trung-tai-ho-chi-minh-kl8-trang-{page_number}-vi.html")
            
            time.sleep(2)

            # Find all job listings on the page (adjust the class selector as per website structure)
            job_items = self.driver.find_elements(By.CLASS_NAME, "job-item")

            if not job_items:
                print("No more job items found, stopping.")
                break  # Exit the loop if no jobs are found

            # Process each job item
            for index, job in enumerate(job_items):
                try:
                    # Extract job title, company, and job URL (adjust CSS selectors based on website structure)
                    job_title_element = job.find_element(By.CSS_SELECTOR, "h2 a")
                    job_title = job_title_element.text
                    job_company = job.find_element(By.CLASS_NAME, "company-name").text
                    job_url = job_title_element.get_attribute("href")
                    salary = job.find_element(By.CLASS_NAME, "salary").text
                    deadline = get_element_text(self.driver, By.CLASS_NAME, "expire-date")
                    location = job.find_element(By.CLASS_NAME, "location").text

                    print(f"Opening job {index + 1}: {job_title}")
                    
                    # Open the job in a new tab to get detailed information
                    self.driver.execute_script("window.open(arguments[0]);", job_url)
                    
                    # Switch to the new tab
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    
                    # Wait for the job page to load (you might need to adjust this CSS selector as well)
                    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))

                    try:
                        # Extract job description from the section you highlighted (adjust the selector)
                        job_description = get_element_text(self.driver, By.CSS_SELECTOR, "div.detail-row.reset-bullet")
                    except:
                        job_description = ''

                    try:
                        # Extract candidate requirements from "Yêu Cầu Công Việc" section
                        requirements_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.detail-row.reset-bullet ul li")
                        candidate_requirements = [req.text for req in requirements_elements]
                        candidate_requirements_text = ', '.join(candidate_requirements)  # Convert the list to a string
                    except:
                        candidate_requirements_text = ''

                    try:
                        # Extract benefits from the "Phúc lợi" section
                        benefit_elements = self.driver.find_elements(By.CSS_SELECTOR, "ul.welfare-list li")
                        benefits = [benefit.text for benefit in benefit_elements]
                        benefits_text = ', '.join(benefits)  # Convert the list to a string
                    except:
                        benefits_text = ''

                    try:
                        # Extract experience from the "Kinh nghiệm" section
                        domain = self.driver.find_element(By.CSS_SELECTOR, "div.detail-box.has-background ul li:nth-child(2) p").text
                    except:
                        domain = ''

                    try:
                        experience = self.driver.find_element(By.XPATH, "//*[@id='tab-1']/section/div[1]/div/div[3]/div/ul/li[2]/p").text
                    except:
                        experience = ''

                    try:
                        # Extract work location from "Địa Điểm Làm Việc" section
                        work_location_element = self.driver.find_element(By.CSS_SELECTOR, "div.detail-row.info-place-detail div.place-name")
                        work_location = work_location_element.text
                    except:
                        work_location = ''

                    try:
                        # Extract the company overview link
                        company_overview_link_element = self.driver.find_element(By.XPATH, "//*[@id='tabs-job-company']/a")
                        company_overview_link = company_overview_link_element.get_attribute("href")
                        # Visit the company overview link to get more details
                        print(f"Opening company overview for {job_company}")
                        self.driver.execute_script("window.open(arguments[0]);", company_overview_link)
                        # Switch to the new tab for the company overview
                        self.driver.switch_to.window(self.driver.window_handles[-1])
                        time.sleep(5)
                        # Extract any company-specific information from the overview page
                        try:
                            # Extract the company size from the "Thông tin công ty" section
                            company_size_element = self.driver.find_element(By.CSS_SELECTOR, "div.content ul li span.mdi-account-supervisor")
                            company_size = company_size_element.find_element(By.XPATH, "..").text.strip()
                        except: 
                            company_size = ''
                    except:
                        company_size = ''                    

                    # Append the extracted job details to the jobs list
                    jobs.append({
                        "Job Title": job_title,
                        "Company Name": job_company,
                        "Job URL": job_url,
                        "Job Description": job_description,
                        "Candidate Requirements": candidate_requirements_text,
                        "Benefits": benefits_text,
                        "Location": location,
                        "Work Location": work_location,
                        "Salary": salary,
                        "Experience": experience,
                        "Application Deadline": deadline,
                        "Domain": domain,
                        "Company Size": company_size
                    })

                    # Close all tabs except the first one
                    while len(self.driver.window_handles) > 1:
                        self.driver.switch_to.window(self.driver.window_handles[-1])
                        self.driver.close()
                    
                    # Switch back to the original tab
                    self.driver.switch_to.window(self.driver.window_handles[0])
                    time.sleep(2)  # Adjust if necessary
                except Exception as e:
                    print(f"Error processing job {index + 1}: {e}")

            # Save the data to CSV after each page
            self.save_to_csv(f"./test/CareerViet_page_{page_number}.csv", jobs)

        return jobs



    def save_to_csv(self, filename, data):
        # Convert list of dictionaries to pandas DataFrame
        df = pd.DataFrame(data)
        # Save DataFrame to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        print("File saved successfully!")

    def close(self):
        self.driver.quit()

if __name__ == "__main__":
    spider = CareerViet()
    jobs = spider.parse()
    spider.close()
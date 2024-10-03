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
            
class TopCV:
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
        self.start_url = "https://www.topcv.vn/"
        self.stores = []

    def parse(self, keyword):
        """ 
        Input: Keyword to search on self.start_url
        Output: Scrape all pages by navigating to next page and extracting job listings.
        """
        base_url = f"https://www.topcv.vn/tim-viec-lam-{keyword.lower().replace(' ', '-')}"
        jobs = []
        
        for page_number in range(10, 11):
            current_url = f"{base_url}?page={page_number}&sba=1"
            print(f"Scraping page {page_number}: {current_url}")
            self.driver.get(current_url)
            
            # Wait for the job listings to load
            wait = WebDriverWait(self.driver, 10)
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".job-list-search-result .job-item-search-result")))
            except Exception as e:
                print(f"Error loading jobs on page {page_number}: {e}")
                break  # Stop if the page cannot load or no jobs are found

            # Find all job listings on the page
            job_items = self.driver.find_elements(By.CLASS_NAME, "job-item-search-result")

            if not job_items:
                print("No more job items found, stopping.")
                break  # Exit the loop if no jobs are found

            # Process each job item
            for index, job in enumerate(job_items):
                try:
                    job_title_element = job.find_element(By.CSS_SELECTOR, "h3.title a")
                    job_title = job_title_element.text
                    job_company = job.find_element(By.CSS_SELECTOR, "a.company").text
                    job_url = job_title_element.get_attribute("href")
                    
                    print(f"Opening job {index + 1}: {job_title}")
                    
                    # Open the job in a new tab
                    self.driver.execute_script("window.open(arguments[0]);", job_url)
                    
                    # Switch to the new tab
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    
                    # Wait for the job page to load
                    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))

                    # Close Ads if present
                    try:
                        wait = WebDriverWait(self.driver, 1)
                        close_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='popup-social-campaign-company']/div/div/div[1]/button")))
                        close_button.click()
                    except Exception as e:
                        print("No modal to close or unable to close the modal:", e)
                    # Extract job details from the job detail page
                    job_description = get_element_text(self.driver, By.CSS_SELECTOR, "div.job-description__item--content")
                    candidate_requirements = get_element_text(self.driver, By.XPATH, "//div[h3[contains(text(),'Yêu cầu ứng viên')]]/div")
                    benefits = get_element_text(self.driver, By.XPATH, "//div[h3[contains(text(),'Quyền lợi')]]/div")
                    work_location = get_element_text(self.driver, By.XPATH, "//div[h3[contains(text(),'Địa điểm làm việc')]]/div")
                    salary = get_element_text(self.driver, By.CSS_SELECTOR, "div.job-detail__info--section-content-value")
                    experience = get_element_text(self.driver, By.XPATH, "//*[@id='job-detail-info-experience']/div[2]/div[2]")
                    deadline = get_element_text(self.driver, By.XPATH, "//*[@id='header-job-info']/div[2]/div")
                    location = get_element_text(self.driver, By.XPATH, "//*[@id='header-job-info']/div[1]/div[2]/div[2]/div[2]")
                    domain = get_element_text(self.driver, By.XPATH, "//*[@id='job-detail']/div[3]/div/div[2]/div[1]/div[1]/div[3]/div[2]")
                    company_size = get_element_text(self.driver, By.CSS_SELECTOR, "div.company-value")

                    jobs.append({
                        "Job Title": job_title,
                        "Company Name": job_company,
                        "Job URL": job_url,
                        "Job Description": job_description,
                        "Candidate Requirements": candidate_requirements,
                        "Benefits": benefits,
                        'Location': location,
                        "Work Location": work_location,
                        "Salary": salary,
                        "Experience": experience,
                        "Application Deadline": deadline,
                        "Domain": domain,
                        "Company Size": company_size
                    })

                    # Close the current tab and return to the original search results tab
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
                    time.sleep(2)
                except Exception as e:
                    print(f"Error processing job {index + 1}: {e}")

            self.save_to_csv(f"TopCV_page_{page_number}.csv", jobs)
            time.sleep(5)
        return jobs

    def save_to_csv(self, filename, data):
        # Convert list of dictionaries to pandas DataFrame
        df = pd.DataFrame(data)
        # Save DataFrame to CSV
        df.to_csv(filename, index=False, encoding='utf-8')

        # # Save DataFrame to an Excel file
        # df.to_excel('job_listings.xlsx', index=False, engine='openpyxl')
        print("File saved successfully!")


    def close(self):
        self.driver.quit()

if __name__ == "__main__":
    spider = TopCV()
    jobs = spider.parse(keyword="tieng trung")
    spider.close()
# import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import openpyxl
import re
import time 

# from xlwt import Workbook
from selenium.webdriver.common.by import By 



class FPTstore_crawler():
    def __init__(self) -> None:
        self.options = Options()
        # self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--start-maximized')  # Set window size for headless mode
        self.driver = webdriver.Chrome(options=self.options)

        self.province_url_list = []
        self.province_district_url_list = []
        self.url_dict = {}
        self.store_lst = []

    def wait_for_document_ready(self, timeout=10):
        WebDriverWait(self.driver, timeout).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )

    def wait_for_element(self, by, value, timeout=5):
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            return None

    def get_element(self, by, value):
        try:
            element = self.driver.find_element(by, value)
            return element
        except NoSuchElementException:
            return None
        
    def get_elements(self, by, value):
        try:
            element = self.driver.find_elements(by, value)
            return element
        except NoSuchElementException:
            return None

    def get_province_urls(self, url):
        self.driver.get(url)
        time.sleep(1)
        #TODO: Rebuild this code
        province = self.get_element(By.CLASS_NAME, 'showListCity')
        province_lst = province.find_elements(By.TAG_NAME,'a')
        self.province_url_list = [tag.get_attribute('href') for tag in province_lst]


    def get_district_urls(self):
        for province_url in self.province_url_list:
            print(province_url)
            self.driver.get(province_url)
            time.sleep(1)
            district_lst = self.get_element(By.CLASS_NAME, 'showListDistricts')
            province_district_list = district_lst.find_elements(By.TAG_NAME, 'a')
            self.province_district_url_list.extend([tag.get_attribute('href') for tag in province_district_list])
            
        time.sleep(1)

    def get_store_urls(self):
        for district in self.province_district_url_list:
            self.driver.get(district)
            time.sleep(1)
            while True:
                try:
                    WebDriverWait(self.driver, 1).until(
                        EC.element_to_be_clickable((By.CLASS_NAME,'btn-more'))
                    )
                    self.driver.find_element(By.CLASS_NAME,'btn-more').click()
                except:
                    break

            parent_element = self.get_element(By.CLASS_NAME, "ShowListShop")
            child_elements = parent_element.find_elements(By.CLASS_NAME, "ShopItem1")
            data_urls = []
            for child in child_elements:
                data_url = child.get_attribute('data-url')
                print(data_url)
                data_urls.append(data_url)
            self.url_dict[district] = data_urls
        
        

    def get_store_details(self):
        def find_iframe(by, value, timeout=15):
            try:
                # Wait for the iframe to be present
                iframe_element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((by, value))
                )

                return iframe_element
            except Exception as e:
                print(f"Failed to locate iframe: {e}")
                return None

        count_total = 0
        
        for district_url, store_urls in self.url_dict.items():
            count_district = 0
            for store_url in store_urls:
                
                print(store_url)
                
                self.driver = webdriver.Chrome(options=self.options)
    
                self.driver.get('https://fptshop.com.vn' + store_url)

                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'shop-address'))
                )

                shop_address = self.get_element(By.CLASS_NAME, 'shop-address')
                if not shop_address:
                    raise ValueError('Fail to get shop address')
                    

                address_item = shop_address.find_element(By.CLASS_NAME, "address-item")
                if not address_item:
                    raise ValueError('Fail to get item address')

                address = self.get_element(By.CLASS_NAME, "text")
                if address:
                    address_text = address.find_element(By.CLASS_NAME, "common-text").text
                
                self.wait_for_document_ready(timeout=20)
                # Wait for the iframe to be present


                iframe_element = find_iframe(By.TAG_NAME, 'iframe')
            
                if not iframe_element:
                    raise ValueError('Fail to get IFRAME')
                

                self.driver.maximize_window()

                # Switch to the iframe context
                self.driver.switch_to.frame(iframe_element)

                try:
                    checking_iframe = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'google-maps-link'))
                    )
                    map_address = self.driver.find_element(By.CLASS_NAME, 'google-maps-link')
                    location = map_address.find_element(By.TAG_NAME, 'a').get_attribute('href')
                except:
                    print('Cannot find location')
                    continue

                pattern = r"ll=([0-9\.\-]+),([0-9\.\-]+)"
                match = re.search(pattern, location)
                if match:
                    latitude = match.group(1)
                    longitude = match.group(2)
                    print(f'({latitude,longitude})')
                self.store_lst.append([district_url.split('/')[-2], address_text, location, latitude, longitude])
            
                # Always switch back to the default content
                # self.driver.switch_to.default_content()
                count_district += 1
                count_total += 1

            print(count_district)

        print(count_total)
                        

                


    def save_to_excel(self, filename):
        wb = openpyxl.Workbook()
        ws = wb.active
        col_name = ['Tỉnh/Thành Phố','Địa chỉ', 'URL google map', 'Latitude', 'Longitude']
        ws.append(col_name)

        for store in self.store_lst:
            ws.append(store)
        wb.save(filename=filename)


    def scrape(self, url, output_file):
        try:
            self.get_province_urls(url)
            self.get_district_urls()
            self.get_store_urls()
            print('\n\n')
            self.get_store_details()
            self.save_to_excel(output_file)
        except Exception as ex:
            print(ex)
        finally:
            self.driver.quit()
# 171799

if __name__=="__main__":
    scraper = FPTstore_crawler()
    scraper.scrape('https://fptshop.com.vn/cua-hang', 'FPT.xlsx')

  
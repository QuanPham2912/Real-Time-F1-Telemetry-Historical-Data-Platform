import requests
from bs4 import BeautifulSoup as bs
import unicodedata
from unidecode import unidecode
import re
from extract.base import Extractor 

class StatsF1(Extractor):

    def __init__(self):
        super().__init__(source_name = "Stats_F1")
        self.BaseURL = "https://www.statsf1.com/en"
        # Thêm headers giả lập trình duyệt Chrome trên Windows
        self.headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,"
                    " like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            }

    def extract_driver_detail(self, season :int):
        result = []
        URL = f"{self.BaseURL}/{season}/pilotes.aspx"
        response = requests.get(URL,headers= self.headers , timeout=10)
        response.raise_for_status()
        soup = bs(response.text, "lxml")
        #print(soup)
        data = soup.find("table", class_ = "datatable")
        table = data.find("tbody")
        rows = table.find_all("tr")
        for row in rows:
            col = row.find_all("td")
            Driver = col[0].text.strip()
            Constructor = col[1].text.strip()
            Engine = col[2].text.strip()
            Best_Result = col[3].text.strip()
            result.append({
                "Driver" : Driver,
                "Constructor" : Constructor,
                "Engine_Manufacturer" : Engine,
                "Best_Result" : Best_Result
            })
        return result

    def extract_car_detail(self, season :int):
        result = []
        URL = f"{self.BaseURL}/{season}/modeles.aspx"
        response = requests.get(URL, headers= self.headers, timeout=10)
        response.raise_for_status()
        soup = bs(response.text, "lxml")
        table = soup.find("table", class_ = "datatable").find("tbody")
        rows = table.find_all("tr")
        for row in rows:
            col = row.find_all("td")
            Constructor = unicodedata.normalize("NFKD",col[0].find("a").text.strip())
            Chassis = unicodedata.normalize("NFKD", col[0].text).strip()
            Engine = unicodedata.normalize("NFKD", col[2].text).strip()

            result.append({
                # tối thêm constructor vào nữa ko có mỗi chassis với engine thì bt đâu mà lần 
                "Constructor" : Constructor,
                "Chassis" : Chassis,
                "Engine" : Engine
            })
        return result

    def extract_engine_supplier_detail(self, seasons :int ):
        URL = f"{self.BaseURL}/moteurs-{seasons}.aspx"
        result = []
        response = requests.get(URL, headers=self.headers, timeout=10)
        response.raise_for_status()
        soup = bs(response.text,"lxml")
        table = soup.find("table", class_ = "sortable").find("tbody")
        rows = table.find_all("tr")
        for row in rows:
            col = row.find_all("td")
            Engine_Manufacturer = col[0].text.strip()
            Nation = col[1]["sorttable_customkey"]
            Started_time = col[2].text.strip()
            result.append({
                "Engine_Manufacturer" : Engine_Manufacturer,
                "Nation" : Nation,
                "Started_time" : Started_time
            })
        return result

    def extract_constructor_nation(self, seasons :int):
        URL =f"{self.BaseURL}/constructeurs-{seasons}.aspx"
        result = []
        response = requests.get(URL, headers=self.headers, timeout=10)
        response.raise_for_status()
        soup = bs(response.text, "lxml")
        table = soup.find("table", class_ = "sortable").find("tbody")
        rows = table.find_all("tr")
        for row in rows:
            col = row.find_all("td")
            Constructor = col[0].text.strip()
            Nation = col[1]["sorttable_customkey"].strip()
            Strated_time = col[2].text.strip()
            result.append({
                "Constructor" : Constructor,
                "Nation" : Nation,
                "Started_time" : Strated_time
            })
        return result

    def clean_gp_title_to_statsf1_slug(self, raw_text):
        # 1. Chuyển về chữ thường và xóa khoảng trắng thừa
        text = raw_text.lower().strip()
        
        # 2. Bỏ các tiền tố (GP / Grand Prix / Grand-Prix) + các giới từ (de, du, des, d', de l', de la...)
        # Pattern giải thích:
        # ^(?:grand[\s-]prix|gp) : Bắt đầu bằng 'gp' hoặc 'grand prix'/'grand-prix'
        # \s*                   : Khoảng trắng tùy chọn
        # (?:de\s+l['’]|de\s+la|de|du|des|d['’])? : Các giới từ tiếng Pháp (nếu có)
        # \s*                   : Khoảng trắng thừa sau giới từ
        pattern = r"^(?:grand[\s-]prix|gp)\s*(?:de\s+l['’]|de\s+la|de|du|des|d['’])?\s*"
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
        
        # 3. Bỏ dấu tiếng Pháp (ï -> i, É -> e, ...)
        slug = unidecode(text)
        
        # 4. Thay khoảng trắng, dấu nháy, hoặc ký tự đặc biệt thành dấu gạch ngang '-'
        slug = re.sub(r"[^\w\s-]", "", slug)  # Bỏ ký tự đặc biệt thừa
        slug = re.sub(r"[\s_]+", "-", slug)   # Thay khoảng trắng/gạch dưới bằng -
        
        return slug.strip("-")


    def get_france_name_circut(self, season :int,round :int):
        URL = f"https://fr.motorsport.com/f1/schedule/{season}/"
        response = requests.get(URL, headers= self.headers, timeout=10)
        response.raise_for_status()
        soup = bs(response.text, "lxml")
        table = soup.find("table", class_ = "ms-schedule-table")
        canceled_item = table.find_all("tbody", class_ = lambda e: e and "canceled" in e)
        for item in canceled_item:
            item.decompose()
        rows = table.find_all("tbody", class_ = "ms-schedule-table__item")
        return self.clean_gp_title_to_statsf1_slug(rows[round - 1].find("span").text)


    def extract_detail_GP_information(self,season :int,round :int):
        URL = f"{self.BaseURL}/{season}/{self.get_france_name_circut(season, round)}/classement.aspx"
        result = []
        response = requests.get(URL, headers= self.headers, timeout=10)
        response.raise_for_status()
        soup = bs(response.text,"lxml")
        table = soup.find("table", class_ = "datatable").find("tbody")
        rows = table.find_all("tr")
        for row in rows:
            col = row.find_all("td")
            Position = col[0].text
            Driver_number = col[1].text
            Driver = col[2].text
            Chassis = col[3].text
            Engine_manufacturer = col[4].text
            Total_lap = col[5].text
            Race_time = unicodedata.normalize("NFKD", col[6].text).strip()
            result.append({
                "Position" : Position,
                "Driver_number" : Driver_number,
                "Driver" : Driver,
                "Chassis" : Chassis,
                "Engine_manufacturer" : Engine_manufacturer,
                "Total_lap" : Total_lap,
                "Race_time" : Race_time
            })

        return result

    def extract(self, seasons :int, round :int):
        result = {
            "Driver" : self.extract_driver_detail(seasons),
            "Constructor" : self.extract_constructor_nation(seasons),
            "Engine_Supplier" : self.extract_engine_supplier_detail(seasons),
            "Car" : self.extract_car_detail(seasons),
            "Result" : self.extract_detail_GP_information(seasons,round)
        }
        return result

    







#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script để kéo dữ liệu địa chỉ sáp nhập từ ThuvienPhapluat.vn
Tác giả: AI Assistant
Ngày tạo: 2025-07-18
"""

import requests
import json
import pandas as pd
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, parse_qs, urlparse
import re
import os
from datetime import datetime

class SapNhapCrawler:
    def __init__(self):
        self.base_url = "https://thuvienphapluat.vn"
        self.search_url = "https://thuvienphapluat.vn/ma-so-thue/tra-cuu-thong-tin-sap-nhap-tinh"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self.data = []
        
    def get_page_content(self, url):
        """Lấy nội dung trang web"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'
            return response.text
        except requests.RequestException as e:
            print(f"Lỗi khi truy cập {url}: {e}")
            return None
    
    def parse_main_page(self):
        """Phân tích trang chính để lấy danh sách tỉnh/thành"""
        print("Đang phân tích trang chính...")
        content = self.get_page_content(self.search_url)
        if not content:
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Tìm các tỉnh/thành trong dropdown hoặc select
        provinces = []
        
        # Tìm select box cho tỉnh/thành
        select_tinh = soup.find('select', {'name': re.compile(r'.*[Tt]inh.*')})
        if select_tinh:
            for option in select_tinh.find_all('option'):
                if option.get('value') and option.get('value') != '':
                    provinces.append({
                        'ma_tinh': option.get('value'),
                        'ten_tinh': option.text.strip()
                    })
        
        # Nếu không tìm thấy select box, tìm trong script hoặc data
        if not provinces:
            # Tìm trong các script tag
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # Tìm pattern cho dữ liệu tỉnh/thành
                    province_pattern = r'(?:provinces|tinh|MaTinh).*?[\[\{]([^}\]]+)[\]\}]'
                    matches = re.findall(province_pattern, script.string, re.IGNORECASE | re.DOTALL)
                    for match in matches:
                        try:
                            # Thử parse JSON
                            data = json.loads(f'[{match}]')
                            for item in data:
                                if isinstance(item, dict) and 'value' in item and 'text' in item:
                                    provinces.append({
                                        'ma_tinh': item['value'],
                                        'ten_tinh': item['text']
                                    })
                        except:
                            continue
        
        # Nếu vẫn không có, thử phương pháp khác
        if not provinces:
            # Tìm các link có pattern MaTinh
            links = soup.find_all('a', href=re.compile(r'MaTinh=\d+'))
            seen_provinces = set()
            for link in links:
                href = link.get('href')
                if href:
                    parsed = urlparse(href)
                    query_params = parse_qs(parsed.query)
                    if 'MaTinh' in query_params:
                        ma_tinh = query_params['MaTinh'][0]
                        if ma_tinh not in seen_provinces:
                            seen_provinces.add(ma_tinh)
                            provinces.append({
                                'ma_tinh': ma_tinh,
                                'ten_tinh': link.text.strip() or f"Tỉnh {ma_tinh}"
                            })
        
        print(f"Tìm thấy {len(provinces)} tỉnh/thành")
        return provinces
    
    def get_xa_phuong_list(self, ma_tinh):
        """Lấy danh sách xã/phường theo mã tỉnh"""
        print(f"Đang lấy danh sách xã/phường cho tỉnh {ma_tinh}...")
        
        # Thử truy cập trực tiếp với mã tỉnh
        url = f"{self.search_url}?MaTinh={ma_tinh}"
        content = self.get_page_content(url)
        if not content:
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        xa_phuong_list = []
        
        # Tìm select box cho xã/phường
        select_xa = soup.find('select', {'name': re.compile(r'.*[Xx]a.*')})
        if select_xa:
            for option in select_xa.find_all('option'):
                if option.get('value') and option.get('value') != '':
                    xa_phuong_list.append({
                        'ma_xa': option.get('value'),
                        'ten_xa': option.text.strip()
                    })
        
        # Nếu không tìm thấy select, tìm trong script
        if not xa_phuong_list:
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    xa_pattern = r'(?:xa|phuong|MaXa).*?[\[\{]([^}\]]+)[\]\}]'
                    matches = re.findall(xa_pattern, script.string, re.IGNORECASE | re.DOTALL)
                    for match in matches:
                        try:
                            data = json.loads(f'[{match}]')
                            for item in data:
                                if isinstance(item, dict) and 'value' in item and 'text' in item:
                                    xa_phuong_list.append({
                                        'ma_xa': item['value'],
                                        'ten_xa': item['text']
                                    })
                        except:
                            continue
        
        # Tìm trong các link
        if not xa_phuong_list:
            links = soup.find_all('a', href=re.compile(r'MaXa=\d+'))
            seen_xa = set()
            for link in links:
                href = link.get('href')
                if href:
                    parsed = urlparse(href)
                    query_params = parse_qs(parsed.query)
                    if 'MaXa' in query_params:
                        ma_xa = query_params['MaXa'][0]
                        if ma_xa not in seen_xa:
                            seen_xa.add(ma_xa)
                            xa_phuong_list.append({
                                'ma_xa': ma_xa,
                                'ten_xa': link.text.strip() or f"Xã {ma_xa}"
                            })
        
        print(f"Tìm thấy {len(xa_phuong_list)} xã/phường cho tỉnh {ma_tinh}")
        return xa_phuong_list
    
    def get_sap_nhap_info(self, ma_tinh, ma_xa=None):
        """Lấy thông tin sáp nhập cho tỉnh/xã cụ thể"""
        if ma_xa:
            url = f"{self.search_url}?MaTinh={ma_tinh}&MaXa={ma_xa}"
            print(f"Đang lấy thông tin sáp nhập cho tỉnh {ma_tinh}, xã {ma_xa}...")
        else:
            url = f"{self.search_url}?MaTinh={ma_tinh}"
            print(f"Đang lấy thông tin sáp nhập cho tỉnh {ma_tinh}...")
            
        content = self.get_page_content(url)
        if not content:
            return None
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Tìm bảng thông tin sáp nhập
        tables = soup.find_all('table')
        sap_nhap_info = {
            'ma_tinh': ma_tinh,
            'ma_xa': ma_xa,
            'url': url,
            'thong_tin_truoc_sap_nhap': '',
            'thong_tin_sau_sap_nhap': '',
            'raw_content': ''
        }
        
        # Tìm thông tin trong các div chứa thông tin sáp nhập
        info_divs = soup.find_all('div', class_=re.compile(r'.*info.*|.*result.*|.*content.*'))
        if not info_divs:
            info_divs = soup.find_all('div')
        
        for div in info_divs:
            text = div.get_text(strip=True)
            if any(keyword in text.lower() for keyword in ['sáp nhập', 'trước sáp nhập', 'sau sáp nhập']):
                sap_nhap_info['raw_content'] += text + '\n'
        
        # Tìm trong bảng nếu có
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        if 'trước sáp nhập' in cell_text.lower():
                            if i + 1 < len(cells):
                                sap_nhap_info['thong_tin_truoc_sap_nhap'] = cells[i + 1].get_text(strip=True)
                        elif 'sau sáp nhập' in cell_text.lower():
                            if i + 1 < len(cells):
                                sap_nhap_info['thong_tin_sau_sap_nhap'] = cells[i + 1].get_text(strip=True)
        
        # Nếu không tìm thấy trong bảng, tìm trong text
        if not sap_nhap_info['thong_tin_truoc_sap_nhap'] and not sap_nhap_info['thong_tin_sau_sap_nhap']:
            full_text = soup.get_text()
            
            # Pattern để tìm thông tin trước và sau sáp nhập
            truoc_pattern = r'(?:trước sáp nhập|cũ)[:\s]*([^\n]+)'
            sau_pattern = r'(?:sau sáp nhập|mới)[:\s]*([^\n]+)'
            
            truoc_match = re.search(truoc_pattern, full_text, re.IGNORECASE)
            sau_match = re.search(sau_pattern, full_text, re.IGNORECASE)
            
            if truoc_match:
                sap_nhap_info['thong_tin_truoc_sap_nhap'] = truoc_match.group(1).strip()
            if sau_match:
                sap_nhap_info['thong_tin_sau_sap_nhap'] = sau_match.group(1).strip()
        
        return sap_nhap_info
    
    def crawl_all_data(self):
        """Kéo toàn bộ dữ liệu"""
        print("Bắt đầu kéo dữ liệu...")
        
        # Lấy danh sách tỉnh/thành
        provinces = self.parse_main_page()
        
        if not provinces:
            print("Không tìm thấy danh sách tỉnh/thành. Thử với các mã tỉnh phổ biến...")
            # Tạo danh sách mã tỉnh phổ biến từ 1-96
            provinces = [{'ma_tinh': str(i), 'ten_tinh': f'Tỉnh {i}'} for i in range(1, 97)]
        
        total_records = 0
        
        for province in provinces:
            ma_tinh = province['ma_tinh']
            ten_tinh = province['ten_tinh']
            
            print(f"\n--- Xử lý {ten_tinh} (Mã: {ma_tinh}) ---")
            
            # Lấy thông tin sáp nhập cấp tỉnh
            tinh_info = self.get_sap_nhap_info(ma_tinh)
            if tinh_info:
                tinh_info['ten_tinh'] = ten_tinh
                tinh_info['cap_hanh_chinh'] = 'Tỉnh/Thành phố'
                self.data.append(tinh_info)
                total_records += 1
            
            # Lấy danh sách xã/phường
            xa_phuong_list = self.get_xa_phuong_list(ma_tinh)
            
            for xa_phuong in xa_phuong_list:
                ma_xa = xa_phuong['ma_xa']
                ten_xa = xa_phuong['ten_xa']
                
                print(f"  Xử lý {ten_xa} (Mã: {ma_xa})")
                
                xa_info = self.get_sap_nhap_info(ma_tinh, ma_xa)
                if xa_info:
                    xa_info['ten_tinh'] = ten_tinh
                    xa_info['ten_xa'] = ten_xa
                    xa_info['cap_hanh_chinh'] = 'Xã/Phường'
                    self.data.append(xa_info)
                    total_records += 1
                
                # Delay để tránh bị block
                time.sleep(0.5)
            
            # Delay giữa các tỉnh
            time.sleep(1)
            
            print(f"Đã xử lý xong {ten_tinh}. Tổng cộng: {total_records} bản ghi")
        
        print(f"\nHoàn thành! Tổng cộng đã kéo được {total_records} bản ghi")
        return self.data
    
    def save_to_excel(self, filename=None):
        """Lưu dữ liệu ra file Excel"""
        if not self.data:
            print("Không có dữ liệu để lưu")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sap_nhap_data_{timestamp}.xlsx"
        
        df = pd.DataFrame(self.data)
        
        # Sắp xếp cột
        columns_order = ['ma_tinh', 'ten_tinh', 'ma_xa', 'ten_xa', 'cap_hanh_chinh',
                        'thong_tin_truoc_sap_nhap', 'thong_tin_sau_sap_nhap', 'url', 'raw_content']
        
        # Chỉ giữ lại các cột có trong dataframe
        existing_columns = [col for col in columns_order if col in df.columns]
        df = df[existing_columns]
        
        # Lưu file
        df.to_excel(filename, index=False, engine='openpyxl')
        print(f"Đã lưu dữ liệu vào file: {filename}")
        print(f"Tổng số bản ghi: {len(df)}")
    
    def save_to_csv(self, filename=None):
        """Lưu dữ liệu ra file CSV"""
        if not self.data:
            print("Không có dữ liệu để lưu")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sap_nhap_data_{timestamp}.csv"
        
        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Đã lưu dữ liệu vào file: {filename}")

def main():
    """Hàm chính"""
    crawler = SapNhapCrawler()
    
    print("=== TOOL KÉO DỮ LIỆU ĐỊA CHỈ SÁP NHẬP ===")
    print("Website: https://thuvienphapluat.vn")
    print("Mục đích: Kéo toàn bộ dữ liệu địa chỉ mới và cũ sau sáp nhập")
    print("=" * 50)
    
    try:
        # Kéo toàn bộ dữ liệu
        data = crawler.crawl_all_data()
        
        if data:
            # Lưu ra file
            crawler.save_to_excel()
            crawler.save_to_csv()
            
            print("\n=== THỐNG KÊ ===")
            df = pd.DataFrame(data)
            print(f"Tổng số bản ghi: {len(df)}")
            
            if 'cap_hanh_chinh' in df.columns:
                print("\nPhân loại theo cấp hành chính:")
                print(df['cap_hanh_chinh'].value_counts())
        else:
            print("Không có dữ liệu nào được kéo về")
            
    except KeyboardInterrupt:
        print("\nĐã dừng chương trình theo yêu cầu người dùng")
        if crawler.data:
            print("Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()
            crawler.save_to_csv()
    except Exception as e:
        print(f"Lỗi trong quá trình thực thi: {e}")
        import traceback
        traceback.print_exc()
        
        if crawler.data:
            print("Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()
            crawler.save_to_csv()

if __name__ == "__main__":
    main()

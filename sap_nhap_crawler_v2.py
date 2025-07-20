#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script cải tiến để kéo dữ liệu địa chỉ sáp nhập từ ThuvienPhapluat.vn
Phiên bản 2.0 - Tối ưu hóa dựa trên phân tích cấu trúc
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
import html

class SapNhapCrawlerV2:
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
        self.provinces = []
        self.failed_requests = []
        
    def get_page_content(self, url):
        """Lấy nội dung trang web với retry mechanism"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                response.encoding = 'utf-8'
                return response.text
            except requests.RequestException as e:
                print(f"Lỗi lần {attempt + 1} khi truy cập {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.failed_requests.append({'url': url, 'error': str(e)})
                    return None
    
    def extract_provinces_from_select(self, soup):
        """Trích xuất danh sách tỉnh từ select box"""
        provinces = []
        select_tinh = soup.find('select', {'name': 'tinh-cu'})
        
        if select_tinh:
            print(f"Tìm thấy select box với {len(select_tinh.find_all('option'))} options")
            for option in select_tinh.find_all('option'):
                value = option.get('value')
                if value and value != '0':  # Bỏ qua option mặc định
                    text = html.unescape(option.text.strip())
                    provinces.append({
                        'ma_tinh': value,
                        'ten_tinh': text
                    })
                    print(f"  Thêm tỉnh: {value} - {text}")
        else:
            print("Không tìm thấy select box tinh-cu")
        
        return provinces
    
    def extract_xa_phuong_from_select(self, soup):
        """Trích xuất danh sách xã/phường từ select box"""
        xa_phuong_list = []
        select_xa = soup.find('select', {'name': 'xa-cu'})
        
        if select_xa:
            for option in select_xa.find_all('option'):
                value = option.get('value')
                if value and value != '0':  # Bỏ qua option mặc định
                    text = html.unescape(option.text.strip())
                    xa_phuong_list.append({
                        'ma_xa': value,
                        'ten_xa': text
                    })
        
        return xa_phuong_list
    
    def extract_xa_phuong_from_links(self, soup):
        """Trích xuất danh sách xã/phường từ các link trong trang"""
        xa_phuong_list = []
        seen_xa = set()
        
        # Tìm các link có pattern MaXa
        links = soup.find_all('a', href=re.compile(r'MaXa=\d+'))
        
        for link in links:
            href = link.get('href')
            if href:
                parsed = urlparse(href)
                query_params = parse_qs(parsed.query)
                if 'MaXa' in query_params:
                    ma_xa = query_params['MaXa'][0]
                    if ma_xa not in seen_xa:
                        seen_xa.add(ma_xa)
                        text = link.text.strip()
                        # Làm sạch text
                        text = re.sub(r'^\d+\.\s*', '', text)  # Bỏ số thứ tự
                        xa_phuong_list.append({
                            'ma_xa': ma_xa,
                            'ten_xa': text
                        })
        
        return xa_phuong_list
    
    def parse_sap_nhap_table(self, soup):
        """Phân tích bảng thông tin sáp nhập"""
        sap_nhap_info = {
            'truoc_sap_nhap': '',
            'sau_sap_nhap': '',
            'chi_tiet': []
        }
        
        # Tìm bảng có chứa thông tin sáp nhập
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            
            # Tìm row có header "Trước sáp nhập" và "Sau sáp nhập"
            for i, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    # Kiểm tra nếu có header
                    headers = [cell.get_text(strip=True).lower() for cell in cells]
                    if any('trước sáp nhập' in header for header in headers) and \
                       any('sau sáp nhập' in header for header in headers):
                        
                        # Lấy dữ liệu từ các row tiếp theo
                        for j in range(i + 1, len(rows)):
                            data_row = rows[j]
                            data_cells = data_row.find_all(['td', 'th'])
                            if len(data_cells) >= 2:
                                truoc = html.unescape(data_cells[0].get_text(strip=True))
                                sau = html.unescape(data_cells[1].get_text(strip=True))
                                
                                if truoc and sau:
                                    sap_nhap_info['chi_tiet'].append({
                                        'truoc': truoc,
                                        'sau': sau
                                    })
                                    
                                    # Nếu là dòng đầu tiên hoặc có thông tin tổng quan
                                    if not sap_nhap_info['truoc_sap_nhap']:
                                        sap_nhap_info['truoc_sap_nhap'] = truoc
                                    if not sap_nhap_info['sau_sap_nhap']:
                                        sap_nhap_info['sau_sap_nhap'] = sau
        
        return sap_nhap_info
    
    def get_all_provinces(self):
        """Lấy danh sách tất cả tỉnh/thành phố"""
        print("Đang lấy danh sách tỉnh/thành phố...")
        
        # Thử với trang chính trước
        content = self.get_page_content(self.search_url)
        if not content:
            print("Không thể truy cập trang chính")
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        provinces = self.extract_provinces_from_select(soup)
        
        # Nếu không tìm thấy, thử với một tỉnh cụ thể
        if not provinces:
            print("Không tìm thấy select box ở trang chính, thử với trang có tham số...")
            url_with_param = f"{self.search_url}?MaTinh=01"
            content2 = self.get_page_content(url_with_param)
            if content2:
                soup2 = BeautifulSoup(content2, 'html.parser')
                provinces = self.extract_provinces_from_select(soup2)
        
        # Nếu vẫn không có, tạo danh sách mặc định
        if not provinces:
            print("Không tìm thấy select box, sử dụng danh sách mặc định...")
            provinces = [
                {'ma_tinh': '01', 'ten_tinh': 'Hà Nội'},
                {'ma_tinh': '79', 'ten_tinh': 'Hồ Chí Minh'},
                {'ma_tinh': '83', 'ten_tinh': 'Vĩnh Long'},
                {'ma_tinh': '89', 'ten_tinh': 'An Giang'},
                {'ma_tinh': '92', 'ten_tinh': 'Cần Thơ'},
            ]
        
        print(f"Tìm thấy {len(provinces)} tỉnh/thành phố")
        self.provinces = provinces
        return provinces
    
    def get_xa_phuong_for_province(self, ma_tinh):
        """Lấy danh sách xã/phường cho một tỉnh"""
        url = f"{self.search_url}?MaTinh={ma_tinh}"
        content = self.get_page_content(url)
        
        if not content:
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Thử lấy từ select box trước
        xa_phuong_list = self.extract_xa_phuong_from_select(soup)
        
        # Nếu không có trong select box, lấy từ links
        if not xa_phuong_list:
            xa_phuong_list = self.extract_xa_phuong_from_links(soup)
        
        return xa_phuong_list
    
    def get_sap_nhap_details(self, ma_tinh, ma_xa=None):
        """Lấy chi tiết thông tin sáp nhập"""
        if ma_xa:
            url = f"{self.search_url}?MaTinh={ma_tinh}&MaXa={ma_xa}"
        else:
            url = f"{self.search_url}?MaTinh={ma_tinh}"
        
        content = self.get_page_content(url)
        if not content:
            return None
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Phân tích bảng sáp nhập
        sap_nhap_info = self.parse_sap_nhap_table(soup)
        
        # Thêm thông tin cơ bản
        result = {
            'ma_tinh': ma_tinh,
            'ma_xa': ma_xa,
            'url': url,
            'truoc_sap_nhap': sap_nhap_info['truoc_sap_nhap'],
            'sau_sap_nhap': sap_nhap_info['sau_sap_nhap'],
            'chi_tiet_json': json.dumps(sap_nhap_info['chi_tiet'], ensure_ascii=False),
            'so_luong_thay_doi': len(sap_nhap_info['chi_tiet'])
        }
        
        return result
    
    def crawl_sample_data(self, max_provinces=5, max_xa_per_province=10):
        """Kéo dữ liệu mẫu để test"""
        print(f"Kéo dữ liệu mẫu: tối đa {max_provinces} tỉnh, {max_xa_per_province} xã/tỉnh")
        
        provinces = self.get_all_provinces()
        if not provinces:
            return []
        
        # Chỉ lấy một số tỉnh đầu tiên để test
        sample_provinces = provinces[:max_provinces]
        
        for province in sample_provinces:
            ma_tinh = province['ma_tinh']
            ten_tinh = province['ten_tinh']
            
            print(f"\n--- Xử lý {ten_tinh} (Mã: {ma_tinh}) ---")
            
            # Lấy thông tin cấp tỉnh
            tinh_info = self.get_sap_nhap_details(ma_tinh)
            if tinh_info:
                tinh_info.update({
                    'ten_tinh': ten_tinh,
                    'ten_xa': '',
                    'cap_hanh_chinh': 'Tỉnh/Thành phố'
                })
                self.data.append(tinh_info)
            
            # Lấy danh sách xã/phường
            xa_phuong_list = self.get_xa_phuong_for_province(ma_tinh)
            print(f"Tìm thấy {len(xa_phuong_list)} xã/phường")
            
            # Chỉ lấy một số xã đầu tiên để test
            sample_xa = xa_phuong_list[:max_xa_per_province]
            
            for xa_phuong in sample_xa:
                ma_xa = xa_phuong['ma_xa']
                ten_xa = xa_phuong['ten_xa']
                
                print(f"  Xử lý {ten_xa} (Mã: {ma_xa})")
                
                xa_info = self.get_sap_nhap_details(ma_tinh, ma_xa)
                if xa_info:
                    xa_info.update({
                        'ten_tinh': ten_tinh,
                        'ten_xa': ten_xa,
                        'cap_hanh_chinh': 'Xã/Phường'
                    })
                    self.data.append(xa_info)
                
                time.sleep(0.3)  # Delay ngắn hơn cho test
            
            time.sleep(0.5)
        
        print(f"\nHoàn thành test! Đã kéo được {len(self.data)} bản ghi")
        return self.data
    
    def crawl_full_data(self):
        """Kéo toàn bộ dữ liệu"""
        print("Bắt đầu kéo TOÀN BỘ dữ liệu...")
        
        provinces = self.get_all_provinces()
        if not provinces:
            return []
        
        total_provinces = len(provinces)
        
        for idx, province in enumerate(provinces, 1):
            ma_tinh = province['ma_tinh']
            ten_tinh = province['ten_tinh']
            
            print(f"\n--- [{idx}/{total_provinces}] Xử lý {ten_tinh} (Mã: {ma_tinh}) ---")
            
            # Lấy thông tin cấp tỉnh
            tinh_info = self.get_sap_nhap_details(ma_tinh)
            if tinh_info:
                tinh_info.update({
                    'ten_tinh': ten_tinh,
                    'ten_xa': '',
                    'cap_hanh_chinh': 'Tỉnh/Thành phố'
                })
                self.data.append(tinh_info)
            
            # Lấy danh sách xã/phường
            xa_phuong_list = self.get_xa_phuong_for_province(ma_tinh)
            print(f"Tìm thấy {len(xa_phuong_list)} xã/phường")
            
            for xa_idx, xa_phuong in enumerate(xa_phuong_list, 1):
                ma_xa = xa_phuong['ma_xa']
                ten_xa = xa_phuong['ten_xa']
                
                if xa_idx % 10 == 0:
                    print(f"  Đang xử lý {xa_idx}/{len(xa_phuong_list)}: {ten_xa}")
                
                xa_info = self.get_sap_nhap_details(ma_tinh, ma_xa)
                if xa_info:
                    xa_info.update({
                        'ten_tinh': ten_tinh,
                        'ten_xa': ten_xa,
                        'cap_hanh_chinh': 'Xã/Phường'
                    })
                    self.data.append(xa_info)
                
                time.sleep(0.5)  # Delay để tránh bị block
            
            print(f"Hoàn thành {ten_tinh}. Tổng cộng: {len(self.data)} bản ghi")
            
            # Lưu checkpoint mỗi 5 tỉnh
            if idx % 5 == 0:
                self.save_checkpoint(f"checkpoint_tinh_{idx}")
            
            time.sleep(1)  # Delay giữa các tỉnh
        
        print(f"\nHOÀN THÀNH! Tổng cộng đã kéo được {len(self.data)} bản ghi")
        return self.data
    
    def save_checkpoint(self, filename_prefix):
        """Lưu checkpoint"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        
        print(f"Đã lưu checkpoint: {filename}")
    
    def save_to_excel(self, filename=None):
        """Lưu dữ liệu ra file Excel"""
        if not self.data:
            print("Không có dữ liệu để lưu")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sap_nhap_data_v2_{timestamp}.xlsx"
        
        df = pd.DataFrame(self.data)
        
        # Sắp xếp cột
        columns_order = [
            'ma_tinh', 'ten_tinh', 'ma_xa', 'ten_xa', 'cap_hanh_chinh',
            'truoc_sap_nhap', 'sau_sap_nhap', 'so_luong_thay_doi',
            'chi_tiet_json', 'url'
        ]
        
        existing_columns = [col for col in columns_order if col in df.columns]
        df = df[existing_columns]
        
        # Lưu file với multiple sheets
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet chính
            df.to_excel(writer, sheet_name='Dữ liệu chính', index=False)
            
            # Sheet thống kê
            if 'cap_hanh_chinh' in df.columns:
                stats_df = df['cap_hanh_chinh'].value_counts().reset_index()
                stats_df.columns = ['Cấp hành chính', 'Số lượng']
                stats_df.to_excel(writer, sheet_name='Thống kê', index=False)
            
            # Sheet tỉnh/thành
            if 'ten_tinh' in df.columns:
                tinh_stats = df.groupby('ten_tinh').size().reset_index(name='Số đơn vị')
                tinh_stats.to_excel(writer, sheet_name='Thống kê tỉnh', index=False)
            
            # Sheet lỗi (nếu có)
            if self.failed_requests:
                error_df = pd.DataFrame(self.failed_requests)
                error_df.to_excel(writer, sheet_name='Lỗi', index=False)
        
        print(f"Đã lưu dữ liệu vào file: {filename}")
        print(f"Tổng số bản ghi: {len(df)}")
        
        return filename

def main():
    """Hàm chính"""
    crawler = SapNhapCrawlerV2()
    
    print("=== TOOL KÉO DỮ LIỆU ĐỊA CHỈ SÁP NHẬP V2.0 ===")
    print("Website: https://thuvienphapluat.vn")
    print("Cải tiến: Phân tích cấu trúc HTML, tối ưu hóa hiệu suất")
    print("=" * 60)
    
    print("\nChọn chế độ:")
    print("1. Test với dữ liệu mẫu (5 tỉnh, 10 xã/tỉnh)")
    print("2. Kéo toàn bộ dữ liệu")
    
    try:
        choice = input("Nhập lựa chọn (1 hoặc 2): ").strip()
        
        if choice == "1":
            data = crawler.crawl_sample_data()
        elif choice == "2":
            confirm = input("Cảnh báo: Sẽ kéo TOÀN BỘ dữ liệu (có thể mất vài giờ). Tiếp tục? (y/N): ").strip().lower()
            if confirm == 'y':
                data = crawler.crawl_full_data()
            else:
                print("Đã hủy.")
                return
        else:
            print("Lựa chọn không hợp lệ. Chạy test mặc định.")
            data = crawler.crawl_sample_data()
        
        if data:
            filename = crawler.save_to_excel()
            
            print(f"\n=== THỐNG KÊ ===")
            df = pd.DataFrame(data)
            print(f"Tổng số bản ghi: {len(df)}")
            
            if 'cap_hanh_chinh' in df.columns:
                print("\nPhân loại theo cấp hành chính:")
                print(df['cap_hanh_chinh'].value_counts())
            
            if 'ten_tinh' in df.columns:
                print(f"\nSố tỉnh/thành phố: {df['ten_tinh'].nunique()}")
            
            if crawler.failed_requests:
                print(f"\nSố request lỗi: {len(crawler.failed_requests)}")
            
            print(f"\nFile kết quả: {filename}")
        else:
            print("Không có dữ liệu nào được kéo về")
            
    except KeyboardInterrupt:
        print("\n\nĐã dừng chương trình theo yêu cầu người dùng")
        if crawler.data:
            print("Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()
    except Exception as e:
        print(f"\nLỗi trong quá trình thực thi: {e}")
        import traceback
        traceback.print_exc()
        
        if crawler.data:
            print("Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()

if __name__ == "__main__":
    main()

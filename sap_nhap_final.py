#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script FINAL để kéo dữ liệu địa chỉ sáp nhập từ ThuvienPhapluat.vn
Phiên bản cuối cùng - Tối ưu hóa và hoàn thiện
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class SapNhapCrawlerFinal:
    def __init__(self, max_workers=3):
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
        self.max_workers = max_workers
        self.lock = threading.Lock()
        
        # Danh sách tỉnh/thành cố định (fallback)
        self.fallback_provinces = [
            {'ma_tinh': '01', 'ten_tinh': 'Hà Nội'},
            {'ma_tinh': '02', 'ten_tinh': 'Hà Giang'},
            {'ma_tinh': '04', 'ten_tinh': 'Cao Bằng'},
            {'ma_tinh': '06', 'ten_tinh': 'Bắc Kạn'},
            {'ma_tinh': '08', 'ten_tinh': 'Tuyên Quang'},
            {'ma_tinh': '10', 'ten_tinh': 'Lào Cai'},
            {'ma_tinh': '11', 'ten_tinh': 'Điện Biên'},
            {'ma_tinh': '12', 'ten_tinh': 'Lai Châu'},
            {'ma_tinh': '14', 'ten_tinh': 'Sơn La'},
            {'ma_tinh': '15', 'ten_tinh': 'Yên Bái'},
            {'ma_tinh': '17', 'ten_tinh': 'Hoà Bình'},
            {'ma_tinh': '19', 'ten_tinh': 'Thái Nguyên'},
            {'ma_tinh': '20', 'ten_tinh': 'Lạng Sơn'},
            {'ma_tinh': '22', 'ten_tinh': 'Quảng Ninh'},
            {'ma_tinh': '24', 'ten_tinh': 'Bắc Giang'},
            {'ma_tinh': '25', 'ten_tinh': 'Phú Thọ'},
            {'ma_tinh': '26', 'ten_tinh': 'Vĩnh Phúc'},
            {'ma_tinh': '27', 'ten_tinh': 'Bắc Ninh'},
            {'ma_tinh': '30', 'ten_tinh': 'Hải Dương'},
            {'ma_tinh': '31', 'ten_tinh': 'Hải Phòng'},
            {'ma_tinh': '33', 'ten_tinh': 'Hưng Yên'},
            {'ma_tinh': '34', 'ten_tinh': 'Thái Bình'},
            {'ma_tinh': '35', 'ten_tinh': 'Hà Nam'},
            {'ma_tinh': '36', 'ten_tinh': 'Nam Định'},
            {'ma_tinh': '37', 'ten_tinh': 'Ninh Bình'},
            {'ma_tinh': '38', 'ten_tinh': 'Thanh Hóa'},
            {'ma_tinh': '40', 'ten_tinh': 'Nghệ An'},
            {'ma_tinh': '42', 'ten_tinh': 'Hà Tĩnh'},
            {'ma_tinh': '44', 'ten_tinh': 'Quảng Bình'},
            {'ma_tinh': '45', 'ten_tinh': 'Quảng Trị'},
            {'ma_tinh': '46', 'ten_tinh': 'Thừa Thiên Huế'},
            {'ma_tinh': '48', 'ten_tinh': 'Đà Nẵng'},
            {'ma_tinh': '49', 'ten_tinh': 'Quảng Nam'},
            {'ma_tinh': '51', 'ten_tinh': 'Quảng Ngãi'},
            {'ma_tinh': '52', 'ten_tinh': 'Bình Định'},
            {'ma_tinh': '54', 'ten_tinh': 'Phú Yên'},
            {'ma_tinh': '56', 'ten_tinh': 'Khánh Hòa'},
            {'ma_tinh': '58', 'ten_tinh': 'Ninh Thuận'},
            {'ma_tinh': '60', 'ten_tinh': 'Bình Thuận'},
            {'ma_tinh': '62', 'ten_tinh': 'Kon Tum'},
            {'ma_tinh': '64', 'ten_tinh': 'Gia Lai'},
            {'ma_tinh': '66', 'ten_tinh': 'Đắk Lắk'},
            {'ma_tinh': '67', 'ten_tinh': 'Đắk Nông'},
            {'ma_tinh': '68', 'ten_tinh': 'Lâm Đồng'},
            {'ma_tinh': '70', 'ten_tinh': 'Bình Phước'},
            {'ma_tinh': '72', 'ten_tinh': 'Tây Ninh'},
            {'ma_tinh': '74', 'ten_tinh': 'Bình Dương'},
            {'ma_tinh': '75', 'ten_tinh': 'Đồng Nai'},
            {'ma_tinh': '77', 'ten_tinh': 'Bà Rịa - Vũng Tàu'},
            {'ma_tinh': '79', 'ten_tinh': 'Hồ Chí Minh'},
            {'ma_tinh': '80', 'ten_tinh': 'Long An'},
            {'ma_tinh': '82', 'ten_tinh': 'Tiền Giang'},
            {'ma_tinh': '83', 'ten_tinh': 'Vĩnh Long'},
            {'ma_tinh': '84', 'ten_tinh': 'Trà Vinh'},
            {'ma_tinh': '86', 'ten_tinh': 'Đồng Tháp'},
            {'ma_tinh': '87', 'ten_tinh': 'An Giang'},
            {'ma_tinh': '89', 'ten_tinh': 'Kiên Giang'},
            {'ma_tinh': '91', 'ten_tinh': 'Cà Mau'},
            {'ma_tinh': '92', 'ten_tinh': 'Cần Thơ'},
            {'ma_tinh': '93', 'ten_tinh': 'Hậu Giang'},
            {'ma_tinh': '94', 'ten_tinh': 'Sóc Trăng'},
            {'ma_tinh': '95', 'ten_tinh': 'Bạc Liêu'},
            {'ma_tinh': '96', 'ten_tinh': 'Cà Mau'}
        ]
        
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
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    with self.lock:
                        self.failed_requests.append({'url': url, 'error': str(e)})
                    return None
    
    def extract_xa_phuong_from_links(self, soup, ma_tinh):
        """Trích xuất danh sách xã/phường từ các link trong trang"""
        xa_phuong_list = []
        seen_xa = set()
        
        # Tìm tất cả các link có pattern MaXa (không cần khớp MaTinh trong regex)
        links = soup.find_all('a', href=re.compile(r'MaXa=\d+'))
        
        for link in links:
            href = link.get('href')
            if href and f'MaTinh={ma_tinh}' in href:  # Kiểm tra MaTinh trong href
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
    
    def parse_sap_nhap_info(self, soup):
        """Phân tích thông tin sáp nhập từ trang"""
        info = {
            'truoc_sap_nhap': '',
            'sau_sap_nhap': '',
            'chi_tiet': [],
            'raw_text': ''
        }
        
        # Lấy toàn bộ text để backup
        full_text = soup.get_text(separator=' ', strip=True)
        info['raw_text'] = full_text[:1000]  # Giới hạn 1000 ký tự
        
        # Tìm bảng có thông tin sáp nhập
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            
            # Tìm header row
            for i, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    cell_texts = [cell.get_text(strip=True).lower() for cell in cells]
                    
                    # Kiểm tra nếu có header trước/sau sáp nhập
                    has_truoc = any('trước sáp nhập' in text for text in cell_texts)
                    has_sau = any('sau sáp nhập' in text for text in cell_texts)
                    
                    if has_truoc and has_sau:
                        # Tìm vị trí cột
                        truoc_col = next((j for j, text in enumerate(cell_texts) if 'trước sáp nhập' in text), 0)
                        sau_col = next((j for j, text in enumerate(cell_texts) if 'sau sáp nhập' in text), 1)
                        
                        # Lấy dữ liệu từ các row tiếp theo
                        for j in range(i + 1, len(rows)):
                            data_row = rows[j]
                            data_cells = data_row.find_all(['td', 'th'])
                            
                            if len(data_cells) > max(truoc_col, sau_col):
                                truoc_text = html.unescape(data_cells[truoc_col].get_text(strip=True))
                                sau_text = html.unescape(data_cells[sau_col].get_text(strip=True))
                                
                                if truoc_text and sau_text:
                                    info['chi_tiet'].append({
                                        'truoc': truoc_text,
                                        'sau': sau_text
                                    })
                                    
                                    # Lấy thông tin tổng quan (dòng đầu tiên)
                                    if not info['truoc_sap_nhap']:
                                        info['truoc_sap_nhap'] = truoc_text
                                    if not info['sau_sap_nhap']:
                                        info['sau_sap_nhap'] = sau_text
                        break
        
        # Nếu không tìm thấy trong bảng, thử tìm trong text
        if not info['truoc_sap_nhap'] and not info['sau_sap_nhap']:
            # Pattern đơn giản để tìm thông tin
            patterns = [
                (r'trước sáp nhập[:\s]*([^\n]+)', 'truoc_sap_nhap'),
                (r'sau sáp nhập[:\s]*([^\n]+)', 'sau_sap_nhap'),
            ]
            
            for pattern, key in patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    info[key] = match.group(1).strip()
        
        return info
    
    def get_xa_phuong_for_province(self, ma_tinh):
        """Lấy danh sách xã/phường cho một tỉnh"""
        url = f"{self.search_url}?MaTinh={ma_tinh}"
        content = self.get_page_content(url)
        
        if not content:
            print(f"    Không thể lấy nội dung từ {url}")
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Debug: Đếm tất cả link có MaXa
        all_xa_links = soup.find_all('a', href=re.compile(r'MaXa=\d+'))
        print(f"    Debug: Tìm thấy {len(all_xa_links)} link có MaXa")
        
        # Debug: Đếm link có MaTinh phù hợp
        relevant_links = [link for link in all_xa_links if f'MaTinh={ma_tinh}' in link.get('href', '')]
        print(f"    Debug: Trong đó {len(relevant_links)} link thuộc tỉnh {ma_tinh}")
        
        xa_phuong_list = self.extract_xa_phuong_from_links(soup, ma_tinh)
        
        if not xa_phuong_list:
            print(f"    Debug: Không tìm thấy xã/phường, có thể do logic extract sai")
            # Thử manual
            if relevant_links:
                print(f"    Debug: Thử extract manual từ {len(relevant_links)} link đầu tiên")
                for link in relevant_links[:3]:
                    href = link.get('href', '')
                    text = link.text.strip()
                    print(f"      Link: {href} -> {text}")
        
        return xa_phuong_list
    
    def get_sap_nhap_details(self, ma_tinh, ma_xa=None, ten_tinh='', ten_xa=''):
        """Lấy chi tiết thông tin sáp nhập"""
        if ma_xa:
            url = f"{self.search_url}?MaTinh={ma_tinh}&MaXa={ma_xa}"
            cap_hanh_chinh = 'Xã/Phường'
        else:
            url = f"{self.search_url}?MaTinh={ma_tinh}"
            cap_hanh_chinh = 'Tỉnh/Thành phố'
        
        content = self.get_page_content(url)
        if not content:
            return None
        
        soup = BeautifulSoup(content, 'html.parser')
        sap_nhap_info = self.parse_sap_nhap_info(soup)
        
        result = {
            'ma_tinh': ma_tinh,
            'ten_tinh': ten_tinh,
            'ma_xa': ma_xa or '',
            'ten_xa': ten_xa,
            'cap_hanh_chinh': cap_hanh_chinh,
            'url': url,
            'truoc_sap_nhap': sap_nhap_info['truoc_sap_nhap'],
            'sau_sap_nhap': sap_nhap_info['sau_sap_nhap'],
            'chi_tiet_json': json.dumps(sap_nhap_info['chi_tiet'], ensure_ascii=False) if sap_nhap_info['chi_tiet'] else '',
            'so_luong_thay_doi': len(sap_nhap_info['chi_tiet']),
            'raw_text_sample': sap_nhap_info['raw_text']
        }
        
        return result
    
    def process_province(self, province, max_xa=None):
        """Xử lý một tỉnh (có thể chạy song song)"""
        ma_tinh = province['ma_tinh']
        ten_tinh = province['ten_tinh']
        
        province_data = []
        
        print(f"Bắt đầu xử lý {ten_tinh} (Mã: {ma_tinh})")
        
        # Lấy thông tin cấp tỉnh
        tinh_info = self.get_sap_nhap_details(ma_tinh, ten_tinh=ten_tinh)
        if tinh_info:
            province_data.append(tinh_info)
        
        # Lấy danh sách xã/phường
        xa_phuong_list = self.get_xa_phuong_for_province(ma_tinh)
        print(f"{ten_tinh}: Tìm thấy {len(xa_phuong_list)} xã/phường")
        
        # Giới hạn số lượng xã nếu cần
        if max_xa and len(xa_phuong_list) > max_xa:
            xa_phuong_list = xa_phuong_list[:max_xa]
            print(f"{ten_tinh}: Giới hạn {max_xa} xã đầu tiên")
        
        # Xử lý từng xã/phường
        for xa_phuong in xa_phuong_list:
            ma_xa = xa_phuong['ma_xa']
            ten_xa = xa_phuong['ten_xa']
            
            xa_info = self.get_sap_nhap_details(ma_tinh, ma_xa, ten_tinh, ten_xa)
            if xa_info:
                province_data.append(xa_info)
            
            time.sleep(0.2)  # Delay nhỏ
        
        print(f"Hoàn thành {ten_tinh}: {len(province_data)} bản ghi")
        return province_data
    
    def crawl_sample_data(self, max_provinces=3, max_xa_per_province=5):
        """Kéo dữ liệu mẫu"""
        print(f"Kéo dữ liệu mẫu: {max_provinces} tỉnh, tối đa {max_xa_per_province} xã/tỉnh")
        
        # Sử dụng danh sách fallback với tỉnh có dữ liệu
        sample_provinces = [
            {'ma_tinh': '83', 'ten_tinh': 'Vĩnh Long'},
            {'ma_tinh': '79', 'ten_tinh': 'Hồ Chí Minh'},
            {'ma_tinh': '01', 'ten_tinh': 'Hà Nội'}
        ][:max_provinces]
        
        for province in sample_provinces:
            province_data = self.process_province(province, max_xa_per_province)
            with self.lock:
                self.data.extend(province_data)
        
        print(f"Hoàn thành mẫu! Tổng: {len(self.data)} bản ghi")
        return self.data
    
    def crawl_full_data(self, use_threading=True):
        """Kéo toàn bộ dữ liệu"""
        print("Kéo TOÀN BỘ dữ liệu...")
        
        provinces = self.fallback_provinces
        
        if use_threading and self.max_workers > 1:
            print(f"Sử dụng {self.max_workers} threads")
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self.process_province, province) for province in provinces]
                
                for i, future in enumerate(as_completed(futures)):
                    try:
                        province_data = future.result()
                        with self.lock:
                            self.data.extend(province_data)
                        print(f"Hoàn thành {i+1}/{len(provinces)} tỉnh")
                    except Exception as e:
                        print(f"Lỗi khi xử lý tỉnh: {e}")
        else:
            # Xử lý tuần tự
            for i, province in enumerate(provinces, 1):
                print(f"[{i}/{len(provinces)}] ", end='')
                province_data = self.process_province(province)
                self.data.extend(province_data)
                
                # Checkpoint mỗi 10 tỉnh
                if i % 10 == 0:
                    self.save_checkpoint(f"checkpoint_{i}")
        
        print(f"HOÀN THÀNH! Tổng: {len(self.data)} bản ghi")
        return self.data
    
    def save_checkpoint(self, filename_prefix):
        """Lưu checkpoint"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        
        print(f"Checkpoint: {filename}")
    
    def clean_data_for_excel(self, df):
        """Làm sạch dữ liệu trước khi lưu Excel"""
        import re
        
        for col in df.columns:
            if df[col].dtype == 'object':  # Text columns
                # Loại bỏ ký tự không hợp lệ
                df[col] = df[col].astype(str).apply(lambda x: re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', str(x)) if pd.notna(x) else '')
                # Giới hạn độ dài chuỗi
                df[col] = df[col].apply(lambda x: x[:32767] if len(str(x)) > 32767 else x)
                # Thay thế null
                df[col] = df[col].fillna('')
        
        return df
    
    def save_to_excel(self, filename=None):
        """Lưu dữ liệu ra file Excel"""
        if not self.data:
            print("Không có dữ liệu để lưu")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sap_nhap_final_{timestamp}.xlsx"
        
        df = pd.DataFrame(self.data)
        
        # Làm sạch dữ liệu
        df = self.clean_data_for_excel(df)
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Sheet chính
                df.to_excel(writer, sheet_name='Dữ liệu sáp nhập', index=False)
                
                # Sheet thống kê
                stats = []
                if 'cap_hanh_chinh' in df.columns:
                    cap_stats = df['cap_hanh_chinh'].value_counts()
                    for cap, count in cap_stats.items():
                        stats.append({'Loại': 'Cấp hành chính', 'Giá trị': cap, 'Số lượng': count})
                
                if 'ten_tinh' in df.columns:
                    tinh_count = df['ten_tinh'].nunique()
                    stats.append({'Loại': 'Tổng số tỉnh/thành', 'Giá trị': '', 'Số lượng': tinh_count})
                
                if 'so_luong_thay_doi' in df.columns:
                    total_changes = df['so_luong_thay_doi'].sum()
                    stats.append({'Loại': 'Tổng số thay đổi', 'Giá trị': '', 'Số lượng': total_changes})
                
                if stats:
                    stats_df = pd.DataFrame(stats)
                    stats_df.to_excel(writer, sheet_name='Thống kê', index=False)
                
                # Sheet lỗi
                if self.failed_requests:
                    error_df = pd.DataFrame(self.failed_requests)
                    error_df.to_excel(writer, sheet_name='Lỗi request', index=False)
            
            print(f"Đã lưu: {filename}")
            print(f"Tổng số bản ghi: {len(df)}")
            
        except Exception as e:
            print(f"Lỗi khi lưu Excel: {e}")
            # Fallback: lưu CSV
            csv_filename = filename.replace('.xlsx', '.csv')
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"Đã lưu CSV thay thế: {csv_filename}")
            filename = csv_filename
        
        return filename

def main():
    """Hàm chính"""
    print("=== TOOL KÉO DỮ LIỆU ĐỊA CHỈ SÁP NHẬP - PHIÊN BẢN CUỐI CÙNG ===")
    print("Website: https://thuvienphapluat.vn")
    print("Tác năng: Kéo toàn bộ dữ liệu địa chỉ mới và cũ sau sáp nhập")
    print("=" * 70)
    
    print("\nTùy chọn:")
    print("1. Test nhanh (3 tỉnh, 5 xã/tỉnh)")
    print("2. Kéo toàn bộ dữ liệu (tuần tự)")
    print("3. Kéo toàn bộ dữ liệu (đa luồng - nhanh hơn)")
    
    try:
        choice = input("\nNhập lựa chọn (1-3): ").strip()
        
        if choice == "1":
            crawler = SapNhapCrawlerFinal(max_workers=1)
            data = crawler.crawl_sample_data()
            
        elif choice == "2":
            confirm = input("Cảnh báo: Sẽ kéo TOÀN BỘ dữ liệu (~60 tỉnh). Có thể mất 2-3 giờ. Tiếp tục? (y/N): ").strip().lower()
            if confirm == 'y':
                crawler = SapNhapCrawlerFinal(max_workers=1)
                data = crawler.crawl_full_data(use_threading=False)
            else:
                print("Đã hủy.")
                return
                
        elif choice == "3":
            confirm = input("Cảnh báo: Sẽ kéo TOÀN BỘ dữ liệu với đa luồng. Có thể gây tải cho server. Tiếp tục? (y/N): ").strip().lower()
            if confirm == 'y':
                max_workers = int(input("Số luồng (1-5, khuyến nghị 3): ") or "3")
                crawler = SapNhapCrawlerFinal(max_workers=max(1, min(5, max_workers)))
                data = crawler.crawl_full_data(use_threading=True)
            else:
                print("Đã hủy.")
                return
        else:
            print("Lựa chọn không hợp lệ. Chạy test mặc định.")
            crawler = SapNhapCrawlerFinal(max_workers=1)
            data = crawler.crawl_sample_data()
        
        # Lưu kết quả
        if data:
            filename = crawler.save_to_excel()
            
            print(f"\n=== KẾT QUẢ ===")
            df = pd.DataFrame(data)
            
            # Thống kê chi tiết
            print(f"📊 Tổng số bản ghi: {len(df)}")
            
            if 'cap_hanh_chinh' in df.columns:
                print(f"\n📋 Phân loại theo cấp:")
                for cap, count in df['cap_hanh_chinh'].value_counts().items():
                    print(f"   {cap}: {count}")
            
            if 'ten_tinh' in df.columns:
                print(f"\n🏛️  Số tỉnh/thành: {df['ten_tinh'].nunique()}")
                
            if 'so_luong_thay_doi' in df.columns:
                total_changes = df['so_luong_thay_doi'].sum()
                print(f"🔄 Tổng số thay đổi: {total_changes}")
            
            print(f"\n💾 File Excel: {filename}")
            
            if crawler.failed_requests:
                print(f"⚠️  Số request lỗi: {len(crawler.failed_requests)}")
            
            print(f"\n✅ HOÀN THÀNH!")
            
        else:
            print("❌ Không có dữ liệu nào được kéo về")
            
    except KeyboardInterrupt:
        print("\n\n🛑 Đã dừng theo yêu cầu người dùng")
        if 'crawler' in locals() and crawler.data:
            print("💾 Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()
            
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        
        if 'crawler' in locals() and crawler.data:
            print("💾 Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()

if __name__ == "__main__":
    main()

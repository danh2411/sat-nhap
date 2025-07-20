
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script cuối cùng - sử dụng dữ liệu có sẵn từ website
"""

import requests
import json
import pandas as pd
import time
import os
from bs4 import BeautifulSoup
import re
import html
from datetime import datetime

class SapNhapCrawlerSimple:
    def __init__(self):
        self.base_url = "https://thuvienphapluat.vn"
        self.search_url = "https://thuvienphapluat.vn/ma-so-thue/tra-cuu-thong-tin-sap-nhap-tinh"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
        })
        self.data = []
        self.provinces = []
        self.xa_phuong_cache = {}
        self.error_log = []  # Lưu các lỗi để xử lý lại
        self.stats = {
            'total_processed': 0,
            'success_count': 0,
            'error_count': 0,
            'rate_limit_count': 0,
            'timeout_count': 0,
            'connection_error_count': 0
        }
    
    def get_provinces_from_html(self):
        """Lấy danh sách tỉnh từ dropdown HTML"""
        print("🌐 Đang lấy danh sách tỉnh từ trang web...")
        
        content = self.get_page_content(self.search_url)
        if not content:
            print("❌ Không thể lấy trang chính")
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        provinces = []
        
        # Tìm select box cho tỉnh - có thể có id="tinh-cu" hoặc tương tự
        selects = soup.find_all('select')
        
        for select in selects:
            options = select.find_all('option')
            
            # Kiểm tra xem có phải select tỉnh không bằng cách xem nội dung option
            is_province_select = False
            for option in options:
                if option.get('value') and option.text:
                    # Kiểm tra pattern tên tỉnh
                    text = option.text.strip()
                    if any(keyword in text.lower() for keyword in ['hà nội', 'hồ chí minh', 'bến tre', 'vĩnh long']):
                        is_province_select = True
                        break
            
            if is_province_select:
                print(f"✅ Tìm thấy select box tỉnh với {len(options)} option")
                
                for option in options:
                    value = option.get('value', '').strip()
                    text = option.text.strip()
                    
                    # Bỏ qua option mặc định
                    if value and value != '0' and text and not text.startswith('--'):
                        # Trích xuất mã tỉnh từ value hoặc id
                        ma_tinh = value
                        
                        # Làm sạch tên tỉnh
                        ten_tinh = text.strip()
                        
                        provinces.append({
                            'ma_tinh': ma_tinh,
                            'ten_tinh': ten_tinh
                        })
                
                break  # Đã tìm thấy select tỉnh
        
        print(f"📊 Tìm thấy {len(provinces)} tỉnh/thành phố")
        
        # Hiển thị một vài tỉnh đầu tiên
        for i, province in enumerate(provinces[:5]):
            print(f"  {i+1}. {province['ma_tinh']}: {province['ten_tinh']}")
        
        if len(provinces) > 5:
            print(f"  ... và {len(provinces) - 5} tỉnh khác")
        
        return provinces
    
    def get_xa_phuong_from_province(self, ma_tinh, ten_tinh=''):
        """Lấy danh sách xã/phường từ một tỉnh cụ thể"""
        
        # Kiểm tra cache
        if ma_tinh in self.xa_phuong_cache:
            return self.xa_phuong_cache[ma_tinh]
        
        print(f"  🔍 Đang lấy danh sách xã/phường cho tỉnh {ma_tinh}...")
        
        # Truy cập trang với mã tỉnh
        url = f"{self.search_url}?MaTinh={ma_tinh}"
        content = self.get_page_content(url, ma_tinh=ma_tinh, ten_tinh=ten_tinh)
        
        if not content:
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        xa_phuong_list = []
        
        # Tìm select box cho xã/phường
        selects = soup.find_all('select')
        
        for select in selects:
            options = select.find_all('option')
            
            # Kiểm tra xem có phải select xã/phường không
            is_xa_select = False
            for option in options:
                if option.text and any(keyword in option.text.lower() for keyword in ['phường', 'xã', 'thị trấn']):
                    is_xa_select = True
                    break
            
            if is_xa_select:
                for option in options:
                    value = option.get('value', '').strip()
                    text = option.text.strip()
                    
                    # Bỏ qua option mặc định
                    if value and value != '0' and text and not text.startswith('--'):
                        xa_phuong_list.append({
                            'ma_xa': value,
                            'ten_xa': text
                        })
                
                break
        
        # Nếu không tìm thấy trong select, thử tìm trong các link
        if not xa_phuong_list:
            links = soup.find_all('a', href=re.compile(r'MaXa=\d+'))
            seen_xa = set()
            
            for link in links:
                href = link.get('href', '')
                if f'MaTinh={ma_tinh}' in href:
                    # Trích xuất mã xã từ URL
                    match = re.search(r'MaXa=(\d+)', href)
                    if match:
                        ma_xa = match.group(1)
                        if ma_xa not in seen_xa:
                            seen_xa.add(ma_xa)
                            text = link.text.strip()
                            
                            # Làm sạch text
                            text = re.sub(r'^\d+\.\s*', '', text)
                            
                            xa_phuong_list.append({
                                'ma_xa': ma_xa,
                                'ten_xa': text
                            })
        
        print(f"    📍 Tìm thấy {len(xa_phuong_list)} xã/phường")
        
        # Cache kết quả
        self.xa_phuong_cache[ma_tinh] = xa_phuong_list
        
        return xa_phuong_list
        """Lấy nội dung trang web"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'
            return response.text
        except Exception as e:
            print(f"❌ Lỗi truy cập {url}: {e}")
            return None
    
    def parse_sap_nhap_info(self, soup):
        """Phân tích thông tin sáp nhập từ trang"""
        info = {
            'truoc_sap_nhap': '',
            'sau_sap_nhap': '',
            'chi_tiet': []
        }
        
        # Tìm bảng có thông tin sáp nhập
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            
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
                                
                                if truoc_text and sau_text and len(truoc_text) > 5 and len(sau_text) > 5:
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
        
        return info
    
    def log_error(self, error_type, url, message, ma_tinh=None, ma_xa=None, ten_tinh=None, ten_xa=None):
        """Ghi log lỗi để có thể xử lý lại sau"""
        error_entry = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'error_type': error_type,
            'url': url,
            'message': str(message),
            'ma_tinh': ma_tinh,
            'ma_xa': ma_xa,
            'ten_tinh': ten_tinh,
            'ten_xa': ten_xa
        }
        self.error_log.append(error_entry)
        
        # Cập nhật stats
        self.stats['error_count'] += 1
        if error_type == 'rate_limit':
            self.stats['rate_limit_count'] += 1
        elif error_type == 'timeout':
            self.stats['timeout_count'] += 1
        elif error_type == 'connection_error':
            self.stats['connection_error_count'] += 1

    def get_page_content(self, url, ma_tinh=None, ma_xa=None, ten_tinh=None, ten_xa=None):
        """Lấy nội dung trang web với retry logic"""
        max_retries = 3
        retry_delay = 2  # Start with 2 seconds
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 429:  # Too Many Requests
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                        print(f"    ⏳ Rate limited. Chờ {wait_time}s rồi thử lại...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"    ❌ Bị rate limit sau {max_retries} lần thử")
                        self.log_error('rate_limit', url, 'Too Many Requests after retries', ma_tinh, ma_xa, ten_tinh, ten_xa)
                        return None
                
                response.raise_for_status()
                response.encoding = 'utf-8'
                return response.text
                
            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    print(f"    ⏳ Timeout, chờ {wait_time}s rồi thử lại...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"    ❌ Timeout sau {max_retries} lần thử")
                    self.log_error('timeout', url, str(e), ma_tinh, ma_xa, ten_tinh, ten_xa)
                    return None
                    
            except requests.exceptions.ConnectionError as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    print(f"    ⏳ Lỗi kết nối, chờ {wait_time}s rồi thử lại...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"    ❌ Lỗi kết nối sau {max_retries} lần thử")
                    self.log_error('connection_error', url, str(e), ma_tinh, ma_xa, ten_tinh, ten_xa)
                    return None
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    print(f"    ⏳ Lỗi request, chờ {wait_time}s rồi thử lại...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"    ❌ Lỗi request sau {max_retries} lần thử: {e}")
                    self.log_error('request_error', url, str(e), ma_tinh, ma_xa, ten_tinh, ten_xa)
                    return None
        
        return None
    
    def get_sap_nhap_details(self, ma_tinh, ma_xa=None, ten_tinh='', ten_xa=''):
        """Lấy chi tiết thông tin sáp nhập"""
        if ma_xa:
            url = f"{self.search_url}?MaTinh={ma_tinh}&MaXa={ma_xa}"
            cap_hanh_chinh = 'Xã/Phường'
        else:
            url = f"{self.search_url}?MaTinh={ma_tinh}"
            cap_hanh_chinh = 'Tỉnh/Thành phố'
        
        print(f"  📄 Đang lấy: {ten_xa or ten_tinh}")
        
        self.stats['total_processed'] += 1
        
        content = self.get_page_content(url, ma_tinh, ma_xa, ten_tinh, ten_xa)
        if not content:
            return None
        
        soup = BeautifulSoup(content, 'html.parser')
        sap_nhap_info = self.parse_sap_nhap_info(soup)
        
        # Kiểm tra xem có thông tin không
        has_info = bool(sap_nhap_info['truoc_sap_nhap'] or sap_nhap_info['sau_sap_nhap'] or sap_nhap_info['chi_tiet'])
        
        if has_info:
            print(f"    ✅ Có thông tin sáp nhập!")
            self.stats['success_count'] += 1
        else:
            print(f"    ⚪ Không có thông tin sáp nhập")
        
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
            'co_thong_tin': has_info
        }
        
        return result
    
    def crawl_all_autodiscovery(self, max_provinces=None):
        """Tự động tìm tất cả tỉnh và xã/phường từ website"""
        print("\n🤖 === BẮT ĐẦU AUTO-DISCOVERY ===")
        
        # Bước 1: Lấy danh sách tỉnh
        provinces = self.get_provinces_from_html()
        
        if not provinces:
            print("❌ Không tìm thấy tỉnh nào. Fallback về dữ liệu mẫu.")
            return self.crawl_known_data()
        
        # Giới hạn số tỉnh nếu cần
        if max_provinces and max_provinces < len(provinces):
            provinces = provinces[:max_provinces]
            print(f"⚠️  Giới hạn xử lý {max_provinces} tỉnh đầu tiên")
        
        print(f"\n🏛️  Sẽ xử lý {len(provinces)} tỉnh")
        
        total_processed = 0
        
        for i, province in enumerate(provinces, 1):
            ma_tinh = province['ma_tinh']
            ten_tinh = province['ten_tinh']
            
            print(f"\n📍 [{i}/{len(provinces)}] Tỉnh: {ten_tinh} (Mã: {ma_tinh})")
            
            # Lấy danh sách xã/phường
            xa_phuong_list = self.get_xa_phuong_from_province(ma_tinh, ten_tinh)
            
            if not xa_phuong_list:
                print(f"  ⚠️  Không có xã/phường nào cho tỉnh {ten_tinh}")
                continue
            
            print(f"  📊 Tìm thấy {len(xa_phuong_list)} xã/phường")
            
            # Xử lý từng xã/phường
            for j, xa in enumerate(xa_phuong_list, 1):
                ma_xa = xa['ma_xa']
                ten_xa = xa['ten_xa']
                
                if j <= 3 or j % 10 == 0:  # Hiển thị progress
                    print(f"    [{j}/{len(xa_phuong_list)}] {ten_xa}")
                
                # Lấy thông tin sáp nhập
                xa_info = self.get_sap_nhap_details(ma_tinh, ma_xa, ten_tinh, ten_xa)
                
                if xa_info:
                    self.data.append(xa_info)
                    total_processed += 1
                
                # Delay để tránh bị block - tăng delay cho auto-discovery
                time.sleep(1.2)  # Tăng từ 0.6s lên 1.2s
            
            print(f"  ✅ Hoàn thành tỉnh {ten_tinh}: {len(xa_phuong_list)} xã/phường")
            
            # Delay giữa các tỉnh - tăng delay
            if i < len(provinces):
                time.sleep(3)  # Tăng từ 1.5s lên 3s
        
        print(f"\n🎉 AUTO-DISCOVERY HOÀN THÀNH!")
        print(f"📊 Tổng cộng xử lý: {total_processed} bản ghi từ {len(provinces)} tỉnh")
        
        return self.data

    def save_error_log(self, filename=None):
        """Lưu danh sách lỗi ra file để xử lý lại"""
        if not self.error_log:
            print("📋 Không có lỗi nào để lưu")
            return None
            
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"error_log_{timestamp}.xlsx"
        
        try:
            error_df = pd.DataFrame(self.error_log)
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Sheet lỗi chi tiết
                error_df.to_excel(writer, sheet_name='Danh sách lỗi', index=False)
                
                # Sheet thống kê lỗi
                error_stats = []
                error_type_counts = error_df['error_type'].value_counts()
                for error_type, count in error_type_counts.items():
                    error_stats.append({'Loại lỗi': error_type, 'Số lượng': count})
                
                if error_stats:
                    stats_df = pd.DataFrame(error_stats)
                    stats_df.to_excel(writer, sheet_name='Thống kê lỗi', index=False)
                
                # Sheet URL retry
                retry_urls = error_df[['url', 'ma_tinh', 'ma_xa', 'ten_tinh', 'ten_xa']].drop_duplicates()
                retry_urls.to_excel(writer, sheet_name='URLs cần retry', index=False)
            
            print(f"📋 Đã lưu error log: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Lỗi lưu error log: {e}")
            # Fallback CSV
            csv_filename = filename.replace('.xlsx', '.csv')
            error_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"📋 Đã lưu error log CSV: {csv_filename}")
            return csv_filename

    def retry_failed_requests(self):
        """Thử lại các request bị lỗi"""
        if not self.error_log:
            print("📋 Không có lỗi nào để retry")
            return []
        
        print(f"\n🔄 === BẮT ĐẦU RETRY {len(self.error_log)} LỖI ===")
        
        retry_data = []
        success_retry = 0
        
        # Lấy danh sách unique URLs cần retry
        unique_errors = {}
        for error in self.error_log:
            key = (error['ma_tinh'], error['ma_xa'])
            if key not in unique_errors:
                unique_errors[key] = error
        
        for i, (key, error) in enumerate(unique_errors.items(), 1):
            ma_tinh = error['ma_tinh']
            ma_xa = error['ma_xa']
            ten_tinh = error['ten_tinh'] or f"Tỉnh {ma_tinh}"
            ten_xa = error['ten_xa'] or f"Xã {ma_xa}"
            
            print(f"\n🔄 [{i}/{len(unique_errors)}] Retry: {ten_xa}")
            
            # Thử lại với delay lớn hơn
            time.sleep(2)
            
            xa_info = self.get_sap_nhap_details(ma_tinh, ma_xa, ten_tinh, ten_xa)
            
            if xa_info:
                retry_data.append(xa_info)
                success_retry += 1
                print(f"    ✅ Retry thành công!")
            else:
                print(f"    ❌ Retry vẫn lỗi")
        
        print(f"\n🎉 RETRY HOÀN THÀNH!")
        print(f"📊 Thành công: {success_retry}/{len(unique_errors)}")
        
        return retry_data

    def print_statistics(self):
        """In thống kê chi tiết"""
        print(f"\n📊 === THỐNG KÊ CHI TIẾT ===")
        print(f"📈 Tổng số request: {self.stats['total_processed']}")
        print(f"✅ Thành công: {self.stats['success_count']}")
        print(f"❌ Lỗi: {self.stats['error_count']}")
        
        if self.stats['error_count'] > 0:
            print(f"\n📋 Chi tiết lỗi:")
            print(f"  🚫 Rate limit: {self.stats['rate_limit_count']}")
            print(f"  ⏰ Timeout: {self.stats['timeout_count']}")
            print(f"  🔌 Connection error: {self.stats['connection_error_count']}")
            
            success_rate = (self.stats['success_count'] / self.stats['total_processed']) * 100
            print(f"\n📈 Tỷ lệ thành công: {success_rate:.1f}%")
        
        if self.error_log:
            print(f"\n💡 Đề xuất:")
            print(f"  - Có {len(self.error_log)} lỗi cần xử lý lại")
            print(f"  - Chạy crawler.retry_failed_requests() để thử lại")
            print(f"  - Chạy crawler.save_error_log() để lưu danh sách lỗi")

    def crawl_known_data(self, max_items=None):
        """Kéo dữ liệu từ danh sách đã biết"""
        print("🚀 Bắt đầu kéo dữ liệu từ danh sách đã biết...")
        
        # Lấy thông tin cấp tỉnh trước
        print("\n🏛️  === THÔNG TIN CẤP TỈNH ===")
        for province in self.provinces:
            ma_tinh = province['ma_tinh']
            ten_tinh = province['ten_tinh']
            
            print(f"\n📍 {ten_tinh} (Mã: {ma_tinh})")
            tinh_info = self.get_sap_nhap_details(ma_tinh, ten_tinh=ten_tinh)
            
            if tinh_info:
                self.data.append(tinh_info)
        
        # Lấy thông tin cấp xã/phường
        print(f"\n🏘️  === THÔNG TIN CẤP XÃ/PHƯỜNG ===")
        
        items_to_process = self.known_data[:max_items] if max_items else self.known_data
        
        for i, item in enumerate(items_to_process, 1):
            ma_tinh = item['ma_tinh']
            ten_tinh = item['ten_tinh']
            ma_xa = item['ma_xa']
            ten_xa = item['ten_xa']
            
            print(f"\n📍 [{i}/{len(items_to_process)}] {ten_xa}")
            
            xa_info = self.get_sap_nhap_details(ma_tinh, ma_xa, ten_tinh, ten_xa)
            
            if xa_info:
                self.data.append(xa_info)
            
            time.sleep(0.5)  # Delay để tránh bị block
        
        print(f"\n🎉 Hoàn thành! Tổng cộng: {len(self.data)} bản ghi")
        return self.data
    
    def save_to_excel(self, filename=None):
        """Lưu dữ liệu ra file Excel"""
        if not self.data:
            print("❌ Không có dữ liệu để lưu")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sap_nhap_simple_{timestamp}.xlsx"
        
        df = pd.DataFrame(self.data)
        
        # Làm sạch dữ liệu
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).apply(lambda x: re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', str(x)))
                df[col] = df[col].apply(lambda x: x[:32767] if len(str(x)) > 32767 else x)
                df[col] = df[col].fillna('')
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Sheet dữ liệu chính
                df.to_excel(writer, sheet_name='Dữ liệu sáp nhập', index=False)
                
                # Sheet thống kê
                stats = []
                
                if 'cap_hanh_chinh' in df.columns:
                    cap_stats = df['cap_hanh_chinh'].value_counts()
                    for cap, count in cap_stats.items():
                        stats.append({'Loại': 'Cấp hành chính', 'Giá trị': cap, 'Số lượng': count})
                
                if 'co_thong_tin' in df.columns:
                    info_stats = df['co_thong_tin'].value_counts()
                    for has_info, count in info_stats.items():
                        label = 'Có thông tin sáp nhập' if has_info else 'Không có thông tin'
                        stats.append({'Loại': 'Thông tin sáp nhập', 'Giá trị': label, 'Số lượng': count})
                
                if 'so_luong_thay_doi' in df.columns:
                    total_changes = df['so_luong_thay_doi'].sum()
                    stats.append({'Loại': 'Tổng số thay đổi', 'Giá trị': '', 'Số lượng': total_changes})
                
                # Thống kê crawling
                stats.append({'Loại': 'Tổng request', 'Giá trị': '', 'Số lượng': self.stats['total_processed']})
                stats.append({'Loại': 'Request thành công', 'Giá trị': '', 'Số lượng': self.stats['success_count']})
                stats.append({'Loại': 'Request lỗi', 'Giá trị': '', 'Số lượng': self.stats['error_count']})
                
                if self.stats['total_processed'] > 0:
                    success_rate = (self.stats['success_count'] / self.stats['total_processed']) * 100
                    stats.append({'Loại': 'Tỷ lệ thành công (%)', 'Giá trị': '', 'Số lượng': round(success_rate, 1)})
                
                # Chi tiết lỗi
                if self.stats['error_count'] > 0:
                    stats.append({'Loại': 'Rate limit errors', 'Giá trị': '', 'Số lượng': self.stats['rate_limit_count']})
                    stats.append({'Loại': 'Timeout errors', 'Giá trị': '', 'Số lượng': self.stats['timeout_count']})
                    stats.append({'Loại': 'Connection errors', 'Giá trị': '', 'Số lượng': self.stats['connection_error_count']})
                
                if stats:
                    stats_df = pd.DataFrame(stats)
                    stats_df.to_excel(writer, sheet_name='Thống kê', index=False)
                
                # Sheet dữ liệu có thông tin sáp nhập
                if 'co_thong_tin' in df.columns:
                    df_with_info = df[df['co_thong_tin'] == True]
                    if len(df_with_info) > 0:
                        df_with_info.to_excel(writer, sheet_name='Có thông tin sáp nhập', index=False)
                
                # Sheet lỗi nếu có
                if self.error_log:
                    error_df = pd.DataFrame(self.error_log)
                    error_df.to_excel(writer, sheet_name='Danh sách lỗi', index=False)
            
            print(f"💾 Đã lưu: {filename}")
            
            # In thống kê
            self.print_statistics()
            
            # Lưu error log riêng nếu có lỗi
            if self.error_log:
                self.save_error_log()
            
        except Exception as e:
            print(f"❌ Lỗi lưu Excel: {e}")
            # Fallback CSV
            csv_filename = filename.replace('.xlsx', '.csv')
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"💾 Đã lưu CSV: {csv_filename}")
            filename = csv_filename
        
        return filename

def main():
    """Hàm chính"""
    print("=== TOOL KÉO DỮ LIỆU ĐỊA CHỈ SÁP NHẬP - PHIÊN BẢN ĐƠN GIẢN ===")
    print("🌐 Website: https://thuvienphapluat.vn")
    print("🎯 Chiến lược: Auto-discovery hoặc dữ liệu mẫu")
    print("=" * 70)
    
    print("\n📋 Tùy chọn:")
    print("1. 🧪 Test với 5 địa chỉ mẫu")
    print("2. 📊 Kéo tất cả dữ liệu mẫu có sẵn")
    print("3. 🌐 Auto-discovery: Tự động tìm tất cả tỉnh/xã")
    print("4. 🔍 Auto-discovery: Chỉ tìm một số tỉnh đầu tiên")
    print("5. 🔄 Retry các lỗi từ lần crawl trước")
    
    try:
        choice = input("\n➤ Nhập lựa chọn (1-5): ").strip()
        
        crawler = SapNhapCrawlerSimple()
        
        if choice == "1":
            data = crawler.crawl_known_data(max_items=5)
            
        elif choice == "2":
            data = crawler.crawl_known_data()
            
        elif choice == "3":
            # Auto-discovery tất cả
            print("\n🚀 Bắt đầu auto-discovery toàn bộ...")
            data = crawler.crawl_all_autodiscovery()
            
        elif choice == "4":
            # Auto-discovery có giới hạn
            limit = input("➤ Nhập số tỉnh muốn crawl (mặc định 5): ").strip()
            try:
                limit = int(limit) if limit else 5
            except:
                limit = 5
            print(f"\n🚀 Bắt đầu auto-discovery {limit} tỉnh đầu tiên...")
            data = crawler.crawl_all_autodiscovery(max_provinces=limit)
            
        elif choice == "5":
            # Retry các lỗi
            print("\n🔄 Bắt đầu retry các lỗi từ lần crawl trước...")
            print("⚠️  Chức năng này cần có file error log từ lần crawl trước")
            
            # Load error log từ file gần nhất
            error_files = [f for f in os.listdir('.') if f.startswith('error_log_') and f.endswith('.xlsx')]
            if error_files:
                latest_error_file = max(error_files, key=lambda x: os.path.getctime(x))
                print(f"📋 Tìm thấy error log: {latest_error_file}")
                
                try:
                    error_df = pd.read_excel(latest_error_file, sheet_name='Danh sách lỗi')
                    crawler.error_log = error_df.to_dict('records')
                    print(f"📋 Loaded {len(crawler.error_log)} lỗi để retry")
                    
                    data = crawler.retry_failed_requests()
                except Exception as e:
                    print(f"❌ Lỗi load error log: {e}")
                    data = []
            else:
                print("❌ Không tìm thấy file error log nào")
                data = []
            
        else:
            print("⚠️  Lựa chọn không hợp lệ. Chạy test mặc định.")
            data = crawler.crawl_known_data(max_items=5)
        
        # Lưu kết quả
        if data:
            filename = crawler.save_to_excel()
            
            print(f"\n📊 === KẾT QUẢ CUỐI CÙNG ===")
            df = pd.DataFrame(data)
            
            print(f"📈 Tổng số bản ghi: {len(df)}")
            
            if 'cap_hanh_chinh' in df.columns:
                print(f"\n🏛️  Phân loại theo cấp:")
                for cap, count in df['cap_hanh_chinh'].value_counts().items():
                    print(f"   {cap}: {count}")
            
            if 'co_thong_tin' in df.columns:
                info_count = df[df['co_thong_tin'] == True]
                print(f"\n✅ Có thông tin sáp nhập: {len(info_count)}")
                no_info_count = df[df['co_thong_tin'] == False]
                print(f"⚪ Không có thông tin: {len(no_info_count)}")
            
            if 'so_luong_thay_doi' in df.columns:
                total_changes = df['so_luong_thay_doi'].sum()
                print(f"🔄 Tổng số thay đổi: {total_changes}")
            
            print(f"\n💾 File kết quả: {filename}")
            print(f"\n🎉 HOÀN THÀNH THÀNH CÔNG!")
            
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

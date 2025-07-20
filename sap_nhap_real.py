#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script thực tế để kéo dữ liệu - sử dụng cách tiếp cận thực tế
"""

import requests
import json
import pandas as pd
import time
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, urlparse
import re
import os
from datetime import datetime
import html

class SapNhapCrawlerReal:
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
        self.failed_requests = []
        
        # Danh sách tỉnh/thành cố định
        self.provinces = [
            {'ma_tinh': '01', 'ten_tinh': 'Hà Nội'},
            {'ma_tinh': '79', 'ten_tinh': 'Hồ Chí Minh'}, 
            {'ma_tinh': '83', 'ten_tinh': 'Vĩnh Long'},  # Đã test có dữ liệu
            {'ma_tinh': '89', 'ten_tinh': 'An Giang'},
            {'ma_tinh': '92', 'ten_tinh': 'Cần Thơ'},
        ]
        
    def get_page_content(self, url):
        """Lấy nội dung trang web"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'
            return response.text
        except requests.RequestException as e:
            self.failed_requests.append({'url': url, 'error': str(e)})
            return None
    
    def discover_xa_phuong_from_known_page(self, ma_tinh):
        """Khám phá danh sách xã/phường từ trang đã biết có dữ liệu"""
        # Danh sách các mã xã/phường mẫu để thử
        sample_xa_codes = ['28996', '28756', '28757', '1', '100', '200']
        
        xa_phuong_list = []
        found_data = False
        
        for sample_xa in sample_xa_codes:
            if found_data:
                break
                
            sample_url = f"{self.search_url}?MaTinh={ma_tinh}&MaXa={sample_xa}"
            content = self.get_page_content(sample_url)
            
            if not content:
                continue
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Tìm tất cả link có MaXa
            xa_links = soup.find_all('a', href=re.compile(r'MaXa=\d+'))
            
            if xa_links:
                print(f"    🔍 Tìm thấy {len(xa_links)} link từ mẫu MaXa={sample_xa}")
                found_data = True
                
                seen_xa = set()
                for link in xa_links:
                    href = link.get('href', '')
                    if f'MaTinh={ma_tinh}' in href:
                        parsed = urlparse(href)
                        query_params = parse_qs(parsed.query)
                        if 'MaXa' in query_params:
                            ma_xa = query_params['MaXa'][0]
                            if ma_xa not in seen_xa:
                                seen_xa.add(ma_xa)
                                text = link.text.strip()
                                text = re.sub(r'^\d+\.\s*', '', text)
                                xa_phuong_list.append({
                                    'ma_xa': ma_xa,
                                    'ten_xa': text
                                })
                break
        
        if not found_data:
            print(f"    ⚠️  Không tìm thấy dữ liệu xã/phường từ các mẫu")
        
    def discover_xa_phuong_by_scanning(self, ma_tinh, max_scan=50):
        """Quét theo range để tìm mã xã/phường hợp lệ"""
        print(f"    🔍 Quét tìm mã xã hợp lệ cho tỉnh {ma_tinh}...")
        
        valid_xa = []
        scan_ranges = [
            range(1, 100),           # Mã nhỏ
            range(1000, 1100),       # Mã trung bình
            range(28000, 29000),     # Mã theo pattern đã thấy
            range(10000, 10100),     # Mã khác
        ]
        
        found_count = 0
        
        for scan_range in scan_ranges:
            if found_count >= max_scan:
                break
                
            for ma_xa in scan_range:
                if found_count >= max_scan:
                    break
                    
                test_url = f"{self.search_url}?MaTinh={ma_tinh}&MaXa={ma_xa}"
                content = self.get_page_content(test_url)
                
                if content:
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # Kiểm tra xem có thông tin thực sự không
                    text = soup.get_text().lower()
                    if any(keyword in text for keyword in ['sáp nhập', 'phường', 'xã', 'thị trấn']):
                        # Có vẻ như có dữ liệu thực
                        title_tag = soup.find('title')
                        if title_tag:
                            title = title_tag.text.strip()
                            if 'tỉnh' not in title.lower():  # Không phải trang tổng quát
                                valid_xa.append({
                                    'ma_xa': str(ma_xa),
                                    'ten_xa': f'Đơn vị hành chính {ma_xa}',
                                    'url': test_url
                                })
                                found_count += 1
                                print(f"      ✓ Tìm thấy MaXa={ma_xa}")
                
                time.sleep(0.1)  # Delay nhỏ
        
        print(f"    📊 Tìm được {found_count} mã xã hợp lệ")
        return valid_xa
    
    def parse_sap_nhap_info(self, soup):
        """Phân tích thông tin sáp nhập"""
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
                    
                    has_truoc = any('trước sáp nhập' in text for text in cell_texts)
                    has_sau = any('sau sáp nhập' in text for text in cell_texts)
                    
                    if has_truoc and has_sau:
                        truoc_col = next((j for j, text in enumerate(cell_texts) if 'trước sáp nhập' in text), 0)
                        sau_col = next((j for j, text in enumerate(cell_texts) if 'sau sáp nhập' in text), 1)
                        
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
                                    
                                    if not info['truoc_sap_nhap']:
                                        info['truoc_sap_nhap'] = truoc_text
                                    if not info['sau_sap_nhap']:
                                        info['sau_sap_nhap'] = sau_text
                        break
        
        return info
    
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
            'so_luong_thay_doi': len(sap_nhap_info['chi_tiet'])
        }
        
        return result
    
    def crawl_province_data(self, province, max_xa=None):
        """Kéo dữ liệu cho một tỉnh"""
        ma_tinh = province['ma_tinh']
        ten_tinh = province['ten_tinh']
        
        print(f"\n--- Xử lý {ten_tinh} (Mã: {ma_tinh}) ---")
        
        province_data = []
        
        # Lấy thông tin cấp tỉnh
        tinh_info = self.get_sap_nhap_details(ma_tinh, ten_tinh=ten_tinh)
        if tinh_info:
            province_data.append(tinh_info)
            print(f"✓ Lấy được thông tin tỉnh")
        
        # Khám phá danh sách xã/phường
        xa_phuong_list = self.discover_xa_phuong_from_known_page(ma_tinh)
        
        # Nếu không tìm thấy bằng cách 1, thử quét
        if not xa_phuong_list:
            print(f"    🔍 Không tìm thấy từ mẫu, thử quét...")
            xa_phuong_list = self.discover_xa_phuong_by_scanning(ma_tinh, max_scan=20)
        
        print(f"🔍 Tìm thấy {len(xa_phuong_list)} xã/phường")
        
        # Giới hạn số lượng để test
        if max_xa and len(xa_phuong_list) > max_xa:
            xa_phuong_list = xa_phuong_list[:max_xa]
            print(f"📊 Giới hạn {max_xa} xã đầu tiên để test")
        
        # Xử lý từng xã/phường
        success_count = 0
        for i, xa_phuong in enumerate(xa_phuong_list):
            ma_xa = xa_phuong['ma_xa']
            ten_xa = xa_phuong['ten_xa']
            
            print(f"  📍 [{i+1}/{len(xa_phuong_list)}] {ten_xa} (Mã: {ma_xa})")
            
            xa_info = self.get_sap_nhap_details(ma_tinh, ma_xa, ten_tinh, ten_xa)
            if xa_info:
                province_data.append(xa_info)
                success_count += 1
                
                # Hiển thị thông tin nếu có
                if xa_info['truoc_sap_nhap'] or xa_info['sau_sap_nhap']:
                    print(f"    ✓ Có thông tin sáp nhập")
            
            time.sleep(0.3)  # Delay
            
            # Status update mỗi 10 item
            if (i + 1) % 10 == 0:
                print(f"    📈 Đã xử lý {i+1}/{len(xa_phuong_list)}")
        
        print(f"✅ Hoàn thành {ten_tinh}: {len(province_data)} bản ghi ({success_count} xã/phường)")
        return province_data
    
    def crawl_sample_data(self, max_provinces=2, max_xa_per_province=10):
        """Kéo dữ liệu mẫu"""
        print(f"🚀 Bắt đầu kéo dữ liệu mẫu: {max_provinces} tỉnh, tối đa {max_xa_per_province} xã/tỉnh")
        
        sample_provinces = self.provinces[:max_provinces]
        
        for province in sample_provinces:
            province_data = self.crawl_province_data(province, max_xa_per_province)
            self.data.extend(province_data)
        
        print(f"🎉 Hoàn thành! Tổng cộng: {len(self.data)} bản ghi")
        return self.data
    
    def crawl_full_data(self):
        """Kéo toàn bộ dữ liệu"""
        print(f"🚀 Bắt đầu kéo TOÀN BỘ dữ liệu cho {len(self.provinces)} tỉnh")
        
        for i, province in enumerate(self.provinces, 1):
            print(f"\n🏛️  [{i}/{len(self.provinces)}] ", end='')
            province_data = self.crawl_province_data(province)
            self.data.extend(province_data)
            
            # Checkpoint mỗi 2 tỉnh
            if i % 2 == 0:
                self.save_checkpoint(f"checkpoint_tinh_{i}")
            
            time.sleep(1)  # Delay giữa các tỉnh
        
        print(f"\n🎉 HOÀN THÀNH TẤT CẢ! Tổng cộng: {len(self.data)} bản ghi")
        return self.data
    
    def save_checkpoint(self, filename_prefix):
        """Lưu checkpoint"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Checkpoint: {filename}")
    
    def save_to_excel(self, filename=None):
        """Lưu dữ liệu ra file Excel"""
        if not self.data:
            print("❌ Không có dữ liệu để lưu")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sap_nhap_real_{timestamp}.xlsx"
        
        df = pd.DataFrame(self.data)
        
        # Làm sạch dữ liệu
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).apply(lambda x: re.sub(r'[^\x20-\x7E\u00A0-\uFFFF]', '', str(x)) if pd.notna(x) else '')
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
                
                if 'ten_tinh' in df.columns:
                    tinh_count = df['ten_tinh'].nunique()
                    stats.append({'Loại': 'Tổng số tỉnh/thành', 'Giá trị': '', 'Số lượng': tinh_count})
                
                if 'so_luong_thay_doi' in df.columns:
                    total_changes = df['so_luong_thay_doi'].sum()
                    stats.append({'Loại': 'Tổng số thay đổi', 'Giá trị': '', 'Số lượng': total_changes})
                
                # Thống kê có thông tin sáp nhập
                df_with_info = df[(df['truoc_sap_nhap'] != '') | (df['sau_sap_nhap'] != '')]
                stats.append({'Loại': 'Có thông tin sáp nhập', 'Giá trị': '', 'Số lượng': len(df_with_info)})
                
                if stats:
                    stats_df = pd.DataFrame(stats)
                    stats_df.to_excel(writer, sheet_name='Thống kê', index=False)
                
                # Sheet chi tiết có thông tin sáp nhập
                if len(df_with_info) > 0:
                    df_with_info.to_excel(writer, sheet_name='Có thông tin sáp nhập', index=False)
                
                # Sheet lỗi
                if self.failed_requests:
                    error_df = pd.DataFrame(self.failed_requests)
                    error_df.to_excel(writer, sheet_name='Lỗi request', index=False)
            
            print(f"💾 Đã lưu: {filename}")
            
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
    print("=== TOOL KÉO DỮ LIỆU ĐỊA CHỈ SÁP NHẬP - PHIÊN BẢN THỰC TẾ ===")
    print("🌐 Website: https://thuvienphapluat.vn")
    print("🎯 Mục tiêu: Kéo dữ liệu địa chỉ sáp nhập từ trang có dữ liệu thực")
    print("=" * 70)
    
    print("\n📋 Tùy chọn:")
    print("1. 🧪 Test với 2 tỉnh, 5 xã/tỉnh")
    print("2. 📊 Kéo dữ liệu tất cả tỉnh có sẵn")
    
    try:
        choice = input("\n➤ Nhập lựa chọn (1-2): ").strip()
        
        crawler = SapNhapCrawlerReal()
        
        if choice == "1":
            data = crawler.crawl_sample_data(max_provinces=2, max_xa_per_province=5)
            
        elif choice == "2":
            confirm = input("⚠️  Cảnh báo: Sẽ kéo dữ liệu từ nhiều tỉnh. Có thể mất 1-2 giờ. Tiếp tục? (y/N): ").strip().lower()
            if confirm == 'y':
                data = crawler.crawl_full_data()
            else:
                print("❌ Đã hủy.")
                return
        else:
            print("⚠️  Lựa chọn không hợp lệ. Chạy test mặc định.")
            data = crawler.crawl_sample_data()
        
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
            
            if 'ten_tinh' in df.columns:
                print(f"\n🗺️  Số tỉnh/thành: {df['ten_tinh'].nunique()}")
                
            # Thống kê có thông tin sáp nhập
            df_with_info = df[(df['truoc_sap_nhap'] != '') | (df['sau_sap_nhap'] != '')]
            print(f"📋 Có thông tin sáp nhập: {len(df_with_info)}")
            
            if 'so_luong_thay_doi' in df.columns:
                total_changes = df['so_luong_thay_doi'].sum()
                print(f"🔄 Tổng số thay đổi: {total_changes}")
            
            print(f"\n💾 File kết quả: {filename}")
            
            if crawler.failed_requests:
                print(f"⚠️  Số request lỗi: {len(crawler.failed_requests)}")
            
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

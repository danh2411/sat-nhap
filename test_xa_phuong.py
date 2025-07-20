#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script test để lấy danh sách xã/phường từ một tỉnh cụ thể
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, urlparse
import re

def test_get_xa_phuong():
    """Test lấy danh sách xã/phường từ tỉnh Vĩnh Long (mã 83)"""
    ma_tinh = "83"
    url = f"https://thuvienphapluat.vn/ma-so-thue/tra-cuu-thong-tin-sap-nhap-tinh?MaTinh={ma_tinh}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    
    print(f"Test URL: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Status: {response.status_code}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Phương pháp 1: Tìm tất cả các link có MaXa
        print("\n=== PHƯƠNG PHÁP 1: TÌM TẤT CẢ LINK CÓ MaXa ===")
        all_xa_links = soup.find_all('a', href=re.compile(r'MaXa=\d+'))
        print(f"Tìm thấy {len(all_xa_links)} link có MaXa")
        
        # Lọc theo MaTinh
        relevant_links = []
        for link in all_xa_links:
            href = link.get('href')
            if f'MaTinh={ma_tinh}' in href:
                relevant_links.append(link)
        
        print(f"Trong đó {len(relevant_links)} link thuộc tỉnh {ma_tinh}")
        
        xa_phuong_list = []
        seen_xa = set()
        
        for link in relevant_links:
            href = link.get('href')
            parsed = urlparse(href)
            query_params = parse_qs(parsed.query)
            
            if 'MaXa' in query_params:
                ma_xa = query_params['MaXa'][0]
                if ma_xa not in seen_xa:
                    seen_xa.add(ma_xa)
                    text = link.text.strip()
                    text = re.sub(r'^\d+\.\s*', '', text)  # Bỏ số thứ tự
                    xa_phuong_list.append({
                        'ma_xa': ma_xa,
                        'ten_xa': text,
                        'full_link': href
                    })
        
        print(f"Kết quả: {len(xa_phuong_list)} xã/phường duy nhất")
        
        # Hiển thị 10 đầu tiên
        for i, xa in enumerate(xa_phuong_list[:10]):
            print(f"  {i+1}. {xa['ma_xa']}: {xa['ten_xa']}")
        
        if len(xa_phuong_list) > 10:
            print(f"  ... và {len(xa_phuong_list) - 10} xã khác")
        
        # Phương pháp 2: Tìm trong danh sách liên kết
        print("\n=== PHƯƠNG PHÁP 2: TÌM TRONG DANH SÁCH ===")
        
        # Tìm các div/ul chứa danh sách
        lists = soup.find_all(['ul', 'ol', 'div'], class_=re.compile(r'list|menu|item'))
        print(f"Tìm thấy {len(lists)} danh sách có thể")
        
        for i, lst in enumerate(lists):
            links_in_list = lst.find_all('a', href=re.compile(r'MaXa=\d+'))
            if links_in_list:
                print(f"  Danh sách {i+1}: {len(links_in_list)} link MaXa")
        
        # Phương pháp 3: Tìm text patterns
        print("\n=== PHƯƠNG PHÁP 3: PATTERN MATCHING ===")
        
        full_text = soup.get_text()
        
        # Tìm pattern "Danh sách"
        danh_sach_pattern = r'danh sách.*?(?:phường|xã|thị trấn).*?:(.*?)(?:\n\n|\r\n\r\n|$)'
        matches = re.findall(danh_sach_pattern, full_text, re.IGNORECASE | re.DOTALL)
        
        print(f"Tìm thấy {len(matches)} pattern 'danh sách'")
        
        # Test một link cụ thể
        if xa_phuong_list:
            print(f"\n=== TEST LINK CỤ THỂ ===")
            test_xa = xa_phuong_list[0]
            test_url = f"https://thuvienphapluat.vn{test_xa['full_link']}"
            print(f"Test URL: {test_url}")
            
            try:
                test_response = requests.get(test_url, headers=headers, timeout=15)
                print(f"Status: {test_response.status_code}")
                print(f"Content length: {len(test_response.text)}")
                
                # Kiểm tra xem có thông tin sáp nhập không
                test_soup = BeautifulSoup(test_response.text, 'html.parser')
                test_text = test_soup.get_text().lower()
                
                keywords = ['trước sáp nhập', 'sau sáp nhập', 'thông tin']
                found_keywords = [kw for kw in keywords if kw in test_text]
                print(f"Keywords found: {found_keywords}")
                
            except Exception as e:
                print(f"Lỗi test link: {e}")
        
        return xa_phuong_list
        
    except Exception as e:
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    print("=== TEST LẤY DANH SÁCH XÃ/PHƯỜNG ===")
    result = test_get_xa_phuong()
    print(f"\nTổng kết: {len(result)} xã/phường")

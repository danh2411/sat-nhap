#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script test để kiểm tra API và cơ chế của trang web
"""

import requests
import json
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, parse_qs, urlparse

def test_single_request():
    """Test một request đơn lẻ"""
    url = "https://thuvienphapluat.vn/ma-so-thue/tra-cuu-thong-tin-sap-nhap-tinh?MaTinh=83&MaXa=28996"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
    }
    
    print(f"Đang test URL: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        print(f"Status Code: {response.status_code}")
        print(f"Content Length: {len(response.text)}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm các thông tin chính
        print("\n=== CẤU TRÚC TRANG ===")
        
        # Tìm title
        title = soup.find('title')
        if title:
            print(f"Title: {title.text.strip()}")
        
        # Tìm các form
        forms = soup.find_all('form')
        print(f"Số lượng form: {len(forms)}")
        
        for i, form in enumerate(forms):
            print(f"Form {i+1}:")
            print(f"  Action: {form.get('action', 'N/A')}")
            print(f"  Method: {form.get('method', 'N/A')}")
            inputs = form.find_all('input')
            selects = form.find_all('select')
            print(f"  Inputs: {len(inputs)}, Selects: {len(selects)}")
        
        # Tìm các select box
        selects = soup.find_all('select')
        print(f"\nSố lượng select box: {len(selects)}")
        
        for i, select in enumerate(selects):
            name = select.get('name', f'select_{i}')
            print(f"Select '{name}':")
            options = select.find_all('option')
            print(f"  Số lượng option: {len(options)}")
            if len(options) <= 10:
                for option in options:
                    value = option.get('value', '')
                    text = option.text.strip()
                    print(f"    {value}: {text}")
            else:
                print(f"    (Quá nhiều option để hiển thị)")
        
        # Tìm các table
        tables = soup.find_all('table')
        print(f"\nSố lượng table: {len(tables)}")
        
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            print(f"Table {i+1}: {len(rows)} rows")
            if rows:
                first_row = rows[0]
                cells = first_row.find_all(['td', 'th'])
                print(f"  Cột đầu tiên: {len(cells)} cells")
                if len(cells) <= 5:
                    for j, cell in enumerate(cells):
                        print(f"    Cell {j+1}: {cell.text.strip()[:50]}...")
        
        # Tìm script chứa dữ liệu
        scripts = soup.find_all('script')
        print(f"\nSố lượng script: {len(scripts)}")
        
        for i, script in enumerate(scripts):
            if script.string and len(script.string) > 100:
                # Tìm các pattern JSON
                json_patterns = [
                    r'\[.*?\]',
                    r'\{.*?\}',
                    r'var\s+\w+\s*=\s*(\[.*?\])',
                    r'var\s+\w+\s*=\s*(\{.*?\})'
                ]
                
                for pattern in json_patterns:
                    matches = re.findall(pattern, script.string, re.DOTALL)
                    if matches:
                        print(f"Script {i+1} có potential JSON data: {len(matches)} matches")
                        break
        
        # Lưu HTML để debug
        with open('debug_page.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"\nĐã lưu HTML debug vào file debug_page.html")
        
        # Tìm các link liên quan
        links = soup.find_all('a', href=True)
        relevant_links = []
        for link in links:
            href = link.get('href')
            if any(keyword in href.lower() for keyword in ['matinh', 'maxa', 'sap-nhap']):
                relevant_links.append({
                    'href': href,
                    'text': link.text.strip()[:100]
                })
        
        print(f"\nCác link liên quan: {len(relevant_links)}")
        for link in relevant_links[:10]:  # Chỉ hiển thị 10 link đầu
            print(f"  {link['href']} - {link['text']}")
            
    except Exception as e:
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()

def find_ajax_endpoints():
    """Tìm các endpoint AJAX"""
    print("\n=== TÌM AJAX ENDPOINTS ===")
    
    # Thử truy cập trang chính
    main_url = "https://thuvienphapluat.vn/ma-so-thue/tra-cuu-thong-tin-sap-nhap-tinh"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    
    try:
        response = requests.get(main_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm trong script
        scripts = soup.find_all('script')
        ajax_urls = set()
        
        for script in scripts:
            if script.string:
                # Tìm các pattern AJAX
                ajax_patterns = [
                    r'["\']([^"\']*\.aspx[^"\']*)["\']',
                    r'["\']([^"\']*ajax[^"\']*)["\']',
                    r'["\']([^"\']*api[^"\']*)["\']',
                    r'url\s*:\s*["\']([^"\']+)["\']',
                    r'\.get\(["\']([^"\']+)["\']',
                    r'\.post\(["\']([^"\']+)["\']'
                ]
                
                for pattern in ajax_patterns:
                    matches = re.findall(pattern, script.string, re.IGNORECASE)
                    for match in matches:
                        if any(keyword in match.lower() for keyword in ['tinh', 'xa', 'sap', 'nhap']):
                            ajax_urls.add(match)
        
        print(f"Tìm thấy {len(ajax_urls)} potential AJAX URLs:")
        for url in sorted(ajax_urls):
            print(f"  {url}")
            
    except Exception as e:
        print(f"Lỗi khi tìm AJAX endpoints: {e}")

def test_different_provinces():
    """Test với các mã tỉnh khác nhau"""
    print("\n=== TEST CÁC MÃ TỈNH KHÁC NHAU ===")
    
    test_provinces = [
        '01', '79', '83', '89', '92'  # Một vài mã tỉnh để test
    ]
    
    base_url = "https://thuvienphapluat.vn/ma-so-thue/tra-cuu-thong-tin-sap-nhap-tinh"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    
    for ma_tinh in test_provinces:
        url = f"{base_url}?MaTinh={ma_tinh}"
        print(f"\nTest mã tỉnh {ma_tinh}: {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            print(f"  Status: {response.status_code}")
            print(f"  Content length: {len(response.text)}")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Tìm thông tin cụ thể
            tables = soup.find_all('table')
            selects = soup.find_all('select')
            
            print(f"  Tables: {len(tables)}, Selects: {len(selects)}")
            
            # Tìm text chứa từ khóa
            text = soup.get_text().lower()
            keywords = ['sáp nhập', 'trước sáp nhập', 'sau sáp nhập', 'tỉnh', 'thành phố']
            found_keywords = [kw for kw in keywords if kw in text]
            print(f"  Keywords found: {found_keywords}")
            
        except Exception as e:
            print(f"  Lỗi: {e}")

if __name__ == "__main__":
    print("=== SCRIPT TEST TRANG WEB SÁP NHẬP ===")
    
    # Test request đơn lẻ
    test_single_request()
    
    # Tìm AJAX endpoints
    find_ajax_endpoints()
    
    # Test các mã tỉnh khác nhau
    test_different_provinces()
    
    print("\n=== HOÀN THÀNH TEST ===")

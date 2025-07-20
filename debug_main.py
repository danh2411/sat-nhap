#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script debug để kiểm tra trang chính
"""

import requests
from bs4 import BeautifulSoup
import html

def debug_main_page():
    """Debug trang chính"""
    url = "https://thuvienphapluat.vn/ma-so-thue/tra-cuu-thong-tin-sap-nhap-tinh"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    
    print(f"Đang kiểm tra trang chính: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Status Code: {response.status_code}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm tất cả select box
        selects = soup.find_all('select')
        print(f"\nTìm thấy {len(selects)} select box:")
        
        for i, select in enumerate(selects):
            name = select.get('name', f'select_{i}')
            id_attr = select.get('id', 'no-id')
            print(f"Select {i+1}: name='{name}', id='{id_attr}'")
            
            options = select.find_all('option')
            print(f"  Có {len(options)} options")
            
            if len(options) <= 20:  # Hiển thị chi tiết nếu ít options
                for option in options:
                    value = option.get('value', '')
                    text = html.unescape(option.text.strip())
                    print(f"    {value}: {text}")
            else:
                # Hiển thị 5 options đầu và cuối
                for j in range(min(5, len(options))):
                    option = options[j]
                    value = option.get('value', '')
                    text = html.unescape(option.text.strip())
                    print(f"    {value}: {text}")
                
                if len(options) > 10:
                    print(f"    ... ({len(options) - 10} options khác)")
                
                for j in range(max(5, len(options) - 5), len(options)):
                    option = options[j]
                    value = option.get('value', '')
                    text = html.unescape(option.text.strip())
                    print(f"    {value}: {text}")
        
        # Lưu HTML để debug
        with open('debug_main_page.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"\nĐã lưu HTML trang chính vào debug_main_page.html")
        
        # Tìm các script có thể chứa data
        scripts = soup.find_all('script')
        print(f"\nTìm thấy {len(scripts)} script tags")
        
        for i, script in enumerate(scripts):
            if script.string and 'tinh' in script.string.lower():
                print(f"Script {i+1} có chứa từ 'tinh'")
                # Hiển thị một phần
                content = script.string[:500] + "..." if len(script.string) > 500 else script.string
                print(f"  Nội dung: {content}")
        
    except Exception as e:
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_main_page()

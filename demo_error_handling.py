#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demo error handling với timeout thấp để tạo lỗi
"""

from sap_nhap_simple import SapNhapCrawlerSimple
import time

def main():
    print("=== DEMO ERROR HANDLING ===")
    print("🌐 Website: https://thuvienphapluat.vn")
    print("🎯 Demo với timeout thấp để tạo lỗi")
    print("=" * 50)
    
    crawler = SapNhapCrawlerSimple()
    
    # Hack để tạo lỗi: giảm timeout xuống rất thấp
    original_get_page_content = crawler.get_page_content
    
    def get_page_content_with_errors(url, ma_tinh=None, ma_xa=None, ten_tinh=None, ten_xa=None):
        """Version với timeout thấp để demo lỗi"""
        # Tạo lỗi timeout cho một số request
        import random
        if random.random() < 0.3:  # 30% chance tạo lỗi
            print(f"    🔧 Demo: Tạo lỗi timeout cho {url}")
            crawler.log_error('timeout', url, 'Demo timeout error', ma_tinh, ma_xa, ten_tinh, ten_xa)
            return None
        
        return original_get_page_content(url, ma_tinh, ma_xa, ten_tinh, ten_xa)
    
    # Thay thế method để demo
    crawler.get_page_content = get_page_content_with_errors
    
    try:
        # Test với Hà Nội - chỉ 10 xã/phường để demo
        provinces = crawler.get_provinces_from_html()
        if provinces:
            ha_noi = provinces[0]  # Hà Nội
            ma_tinh = ha_noi['ma_tinh']
            ten_tinh = ha_noi['ten_tinh']
            
            print(f"\n📍 Demo với {ten_tinh} (Mã: {ma_tinh})")
            
            xa_phuong_list = crawler.get_xa_phuong_from_province(ma_tinh, ten_tinh)
            
            if xa_phuong_list:
                # Chỉ lấy 10 xã/phường đầu tiên để demo
                demo_list = xa_phuong_list[:10]
                print(f"📊 Demo với {len(demo_list)} xã/phường đầu tiên")
                
                for i, xa in enumerate(demo_list, 1):
                    ma_xa = xa['ma_xa']
                    ten_xa = xa['ten_xa']
                    
                    print(f"\n  [{i}/{len(demo_list)}] {ten_xa}")
                    
                    xa_info = crawler.get_sap_nhap_details(ma_tinh, ma_xa, ten_tinh, ten_xa)
                    
                    if xa_info:
                        crawler.data.append(xa_info)
                    
                    time.sleep(0.5)  # Delay ngắn cho demo
        
        print(f"\n📊 === KẾT QUẢ DEMO ===")
        print(f"📈 Dữ liệu thành công: {len(crawler.data)}")
        print(f"❌ Số lỗi tạo ra: {len(crawler.error_log)}")
        
        # Lưu dữ liệu với thống kê lỗi
        if crawler.data or crawler.error_log:
            filename = crawler.save_to_excel()
            print(f"💾 File kết quả: {filename}")
        
        # Demo retry nếu có lỗi
        if crawler.error_log:
            print(f"\n🔄 === DEMO RETRY ===")
            print(f"Có {len(crawler.error_log)} lỗi, thử retry...")
            
            # Khôi phục method gốc để retry thành công
            crawler.get_page_content = original_get_page_content
            
            retry_data = crawler.retry_failed_requests()
            
            if retry_data:
                print(f"✅ Retry thành công {len(retry_data)} bản ghi")
                # Merge với dữ liệu chính
                all_data = crawler.data + retry_data
                crawler.data = all_data
                
                # Lưu file cuối cùng
                final_filename = crawler.save_to_excel()
                print(f"💾 File cuối cùng: {final_filename}")
        
        print(f"\n🎉 DEMO HOÀN THÀNH!")
            
    except KeyboardInterrupt:
        print("\n\n🛑 Đã dừng theo yêu cầu người dùng")
        if crawler.data or crawler.error_log:
            print("💾 Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()

if __name__ == "__main__":
    main()

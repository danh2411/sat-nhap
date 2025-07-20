#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test thống kê lỗi và retry functionality
"""

from sap_nhap_simple import SapNhapCrawlerSimple

def main():
    print("=== TEST THỐNG KÊ LỖI VÀ RETRY ===")
    print("🌐 Website: https://thuvienphapluat.vn")
    print("🎯 Test với 2 tỉnh đầu tiên để demo lỗi")
    print("=" * 50)
    
    crawler = SapNhapCrawlerSimple()
    
    try:
        # Test với 2 tỉnh đầu tiên - sẽ có một số lỗi rate limit
        data = crawler.crawl_all_autodiscovery(max_provinces=2)
        
        if data or crawler.error_log:
            print(f"\n📊 === KẾT QUẢ TEST ===")
            print(f"📈 Dữ liệu thành công: {len(data)}")
            print(f"❌ Số lỗi gặp phải: {len(crawler.error_log)}")
            
            # Lưu dữ liệu chính
            if data:
                filename = crawler.save_to_excel()
                print(f"💾 File kết quả: {filename}")
            
            # Demo retry nếu có lỗi
            if crawler.error_log:
                print(f"\n🔄 === DEMO RETRY ===")
                retry_choice = input("Có muốn thử retry các lỗi không? (y/n): ").strip().lower()
                
                if retry_choice == 'y':
                    retry_data = crawler.retry_failed_requests()
                    
                    if retry_data:
                        print(f"✅ Retry thành công {len(retry_data)} bản ghi")
                        # Merge với dữ liệu chính
                        all_data = data + retry_data
                        crawler.data = all_data
                        
                        # Lưu file cuối cùng
                        final_filename = crawler.save_to_excel()
                        print(f"💾 File cuối cùng: {final_filename}")
                    else:
                        print("❌ Retry không thành công")
            
            print(f"\n🎉 TEST HOÀN THÀNH!")
            
        else:
            print("❌ Không có dữ liệu nào được kéo về")
            
    except KeyboardInterrupt:
        print("\n\n🛑 Đã dừng theo yêu cầu người dùng")
        if crawler.data or crawler.error_log:
            print("💾 Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()
            
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        if crawler.data or crawler.error_log:
            print("💾 Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()

if __name__ == "__main__":
    main()

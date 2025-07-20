#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Auto-discovery với giới hạn để kiểm tra hiệu quả
"""

from sap_nhap_simple import SapNhapCrawlerSimple

def main():
    print("=== TEST AUTO-DISCOVERY VỚI GIỚI HẠN ===")
    print("🌐 Website: https://thuvienphapluat.vn")
    print("🎯 Test với 3 tỉnh đầu tiên")
    print("=" * 50)
    
    crawler = SapNhapCrawlerSimple()
    
    try:
        # Test với 3 tỉnh đầu tiên
        data = crawler.crawl_all_autodiscovery(max_provinces=3)
        
        if data:
            filename = crawler.save_to_excel()
            print(f"\n📊 === KẾT QUẢ TEST ===")
            print(f"📈 Tổng số bản ghi: {len(data)}")
            print(f"💾 File kết quả: {filename}")
            print(f"\n🎉 TEST HOÀN THÀNH!")
        else:
            print("❌ Không có dữ liệu nào được kéo về")
            
    except KeyboardInterrupt:
        print("\n\n🛑 Đã dừng theo yêu cầu người dùng")
        if crawler.data:
            print("💾 Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()
            
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        if crawler.data:
            print("💾 Đang lưu dữ liệu đã kéo được...")
            crawler.save_to_excel()

if __name__ == "__main__":
    main()

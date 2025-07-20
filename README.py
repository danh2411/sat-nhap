#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
README - Hướng dẫn sử dụng tool crawl data sáp nhập địa chính
"""

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    TOOL CRAWL DỮ LIỆU SÁP NHẬP ĐỊA CHÍNH                    ║
║                           Version 2.0 - Enhanced                            ║
╚══════════════════════════════════════════════════════════════════════════════╝

🎯 TÍNH NĂNG CHÍNH:
▪️ Auto-discovery: Tự động tìm tất cả 63 tỉnh/thành phố và xã/phường
▪️ Thống kê lỗi chi tiết: Rate limit, timeout, connection errors
▪️ Retry functionality: Thử lại các request bị lỗi
▪️ Excel export với multiple sheets và thống kê
▪️ Error logging: Lưu danh sách lỗi để xử lý sau

📊 OUTPUT FILES:
▪️ sap_nhap_simple_YYYYMMDD_HHMMSS.xlsx - Dữ liệu chính
  - Sheet "Dữ liệu sáp nhập": Toàn bộ dữ liệu
  - Sheet "Thống kê": Số liệu tổng quan + thống kê lỗi
  - Sheet "Có thông tin sáp nhập": Chỉ những địa chỉ có thông tin
  - Sheet "Danh sách lỗi": Chi tiết các lỗi gặp phải

▪️ error_log_YYYYMMDD_HHMMSS.xlsx - Log lỗi chi tiết
  - Sheet "Danh sách lỗi": Chi tiết từng lỗi
  - Sheet "Thống kê lỗi": Thống kê theo loại lỗi
  - Sheet "URLs cần retry": Danh sách URL cần thử lại

🚀 CÁCH SỬ DỤNG:

1. CHẠY SCRIPT CHÍNH:
   python sap_nhap_simple.py
   
   Tùy chọn:
   1. Test với 5 địa chỉ mẫu (nhanh)
   2. Crawl dữ liệu mẫu có sẵn 
   3. Auto-discovery toàn bộ (63 tỉnh, ~4-6 giờ)
   4. Auto-discovery giới hạn (test với vài tỉnh)
   5. Retry các lỗi từ lần crawl trước

2. DEMO VÀ TEST:
   python test_auto_discovery.py      # Test với 3 tỉnh
   python test_error_handling.py      # Test error handling
   python demo_error_handling.py      # Demo retry functionality

📈 THỐNG KÊ VÀ MONITORING:

Script sẽ hiển thị real-time:
▪️ Số tỉnh/xã đã xử lý
▪️ Tỷ lệ thành công/lỗi
▪️ Loại lỗi gặp phải
▪️ Thời gian ước tính còn lại

🔄 XỬ LÝ LỖI VÀ RETRY:

Nếu gặp lỗi:
1. Script tự động lưu error log
2. Chọn option 5 để retry các lỗi
3. Hoặc dùng code:
   
   from sap_nhap_simple import SapNhapCrawlerSimple
   crawler = SapNhapCrawlerSimple()
   
   # Load error log từ file
   import pandas as pd
   error_df = pd.read_excel('error_log_latest.xlsx', sheet_name='Danh sách lỗi')
   crawler.error_log = error_df.to_dict('records')
   
   # Retry
   retry_data = crawler.retry_failed_requests()
   
   # Lưu kết quả
   crawler.data = retry_data
   crawler.save_to_excel()

⚡ PERFORMANCE TIPS:

▪️ Delay giữa requests: 1.2s (tránh rate limit)
▪️ Delay giữa tỉnh: 3s 
▪️ Timeout: 15s với 3 lần retry
▪️ Exponential backoff cho rate limiting

Ước tính thời gian:
- 5 địa chỉ test: ~10 giây
- 1 tỉnh (~200 xã): ~5-10 phút  
- Toàn bộ 63 tỉnh: ~4-6 giờ

🛠️ TROUBLESHOOTING:

▪️ Lỗi 429 (Too Many Requests): Script tự động retry với delay tăng dần
▪️ Timeout: Tăng timeout trong get_page_content()
▪️ Connection error: Kiểm tra internet, script sẽ retry tự động
▪️ Excel error: Script tự động fallback sang CSV

📞 SUPPORT:
Nếu gặp vấn đề, kiểm tra:
1. File error_log_*.xlsx để xem lỗi chi tiết
2. Sheet "Thống kê" trong file kết quả
3. Thử chạy retry với option 5

🎉 CHÚC BẠN CRAWL THÀNH CÔNG!
""")

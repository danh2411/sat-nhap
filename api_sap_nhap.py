from flask import Flask, request, jsonify, render_template_string
import pandas as pd

# Đọc dữ liệu từ file Excel
EXCEL_FILE = 'sap_nhap_backup.xlsx'
df = pd.read_excel(EXCEL_FILE)

# Lấy danh sách tỉnh/thành phố
provinces = df[['ma_tinh', 'ten_tinh']].drop_duplicates().sort_values('ten_tinh').to_dict(orient='records')

# Trang HTML giao diện
HTML_FORM = '''
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <title>Tra cứu địa chỉ sau sáp nhập</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .form-group { margin-bottom: 16px; }
        label { font-weight: bold; }
        select, input { padding: 6px; min-width: 200px; }
        button { padding: 8px 16px; }
        .result { margin-top: 24px; background: #f8f8f8; padding: 16px; border-radius: 6px; }
    </style>
</head>
<body>
    <h2>Tra cứu địa chỉ sau sáp nhập</h2>
    <form id="searchForm">
        <div class="form-group">
            <label for="province">Tỉnh/Thành phố:</label>
            <select id="province" name="ma_tinh" required>
                <option value="">-- Chọn tỉnh/thành phố --</option>
                {% for p in provinces %}
                <option value="{{p['ma_tinh']}}">{{p['ten_tinh']}}</option>
                {% endfor %}
            </select>
        </div>
        <div class="form-group">
            <label for="xa">Xã/Phường/Thị trấn:</label>
            <select id="xa" name="ma_xa" required>
                <option value="">-- Chọn xã/phường --</option>
            </select>
        </div>
        <button type="submit">Tra cứu</button>
    </form>
    <div class="result" id="result"></div>
    <script>
        // Khi chọn tỉnh, load xã/phường
        document.getElementById('province').addEventListener('change', function() {
            var ma_tinh = this.value;
            var xaSelect = document.getElementById('xa');
            xaSelect.innerHTML = '<option value="">-- Đang tải xã/phường --</option>';
            if(ma_tinh) {
                fetch('/get-xa?ma_tinh=' + ma_tinh)
                .then(res => res.json())
                .then(data => {
                    xaSelect.innerHTML = '<option value="">-- Chọn xã/phường --</option>';
                    data.forEach(function(xa) {
                        xaSelect.innerHTML += `<option value="${xa.ma_xa}">${xa.ten_xa}</option>`;
                    });
                });
            } else {
                xaSelect.innerHTML = '<option value="">-- Chọn xã/phường --</option>';
            }
        });
        // Xử lý submit form
        document.getElementById('searchForm').addEventListener('submit', function(e) {
            e.preventDefault();
            var ma_tinh = document.getElementById('province').value;
            var ma_xa = document.getElementById('xa').value;
            var resultDiv = document.getElementById('result');
            resultDiv.innerHTML = 'Đang tra cứu...';
            fetch(`/tra-cuu?ma_tinh=${ma_tinh}&ma_xa=${ma_xa}`)
            .then(res => res.json())
            .then(data => {
                if(data.result && data.result.length > 0) {
                    var info = data.result[0];
                    resultDiv.innerHTML = `<b>Địa chỉ trước sáp nhập:</b> ${info.truoc_sap_nhap}<br>
                        <b>Địa chỉ sau sáp nhập:</b> ${info.sau_sap_nhap}`;
                } else {
                    resultDiv.innerHTML = 'Không tìm thấy địa chỉ phù hợp.';
                }
            })
            .catch(() => { resultDiv.innerHTML = 'Lỗi tra cứu.'; });
        });
    </script>
</body>
</html>
'''

app = Flask(__name__)

@app.route('/')
def index():
    return render_template_string(HTML_FORM, provinces=provinces)

@app.route('/get-xa')
def get_xa():
    ma_tinh = request.args.get('ma_tinh')
    xa_list = df[df['ma_tinh'].astype(str) == str(ma_tinh)][['ma_xa', 'ten_xa']].drop_duplicates().sort_values('ten_xa')
    return jsonify(xa_list.to_dict(orient='records'))

@app.route('/tra-cuu', methods=['GET'])
def tra_cuu():
    """
    Tra cứu địa chỉ cũ ra địa chỉ sau sáp nhập
    Truyền vào: ma_tinh, ma_xa hoặc ten_tinh, ten_xa hoặc truoc_sap_nhap
    """
    ma_tinh = request.args.get('ma_tinh')
    ma_xa = request.args.get('ma_xa')
    ten_tinh = request.args.get('ten_tinh')
    ten_xa = request.args.get('ten_xa')
    truoc = request.args.get('truoc_sap_nhap')
    
    # Lọc theo các trường truyền vào
    query = df.copy()
    if ma_tinh:
        query = query[query['ma_tinh'].astype(str) == str(ma_tinh)]
    if ma_xa:
        query = query[query['ma_xa'].astype(str) == str(ma_xa)]
    if ten_tinh:
        query = query[query['ten_tinh'].str.lower().str.contains(ten_tinh.lower())]
    if ten_xa:
        query = query[query['ten_xa'].str.lower().str.contains(ten_xa.lower())]
    if truoc:
        query = query[query['truoc_sap_nhap'].str.lower().str.contains(truoc.lower())]
    
    # Trả về kết quả
    if query.empty:
        return jsonify({'result': None, 'message': 'Không tìm thấy địa chỉ phù hợp.'}), 404
    
    # Chỉ trả về các trường quan trọng
    result = query[['ma_tinh', 'ten_tinh', 'ma_xa', 'ten_xa', 'truoc_sap_nhap', 'sau_sap_nhap']].to_dict(orient='records')
    return jsonify({'result': result, 'count': len(result)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

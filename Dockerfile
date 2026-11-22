# Dockerfile
FROM apache/spark:4.0.2-python3

USER root

# Cài đặt thư viện Python bổ sung nếu cần
RUN pip install --no-cache-dir numpy

# Thiết lập thư mục làm việc trong Container
WORKDIR /opt/spark/work-dir

# Copy file từ VS Code vào trong Container theo phân cấp mới
COPY src/bank_job.py ./
COPY data/bank-additional-full.csv ./data/

# Cấp quyền sở hữu thư mục cho user spark để Kubernetes chạy không bị lỗi Permission
RUN chown -R spark:spark /opt/spark/work-dir

USER spark
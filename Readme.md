# Bank Marketing — Spark trên Kubernetes

<p align="center">
  <img src="docs/logo.webp" alt="Kubernetes + Apache Spark" width="400"/>
</p>

---

## Dự án này giải quyết vấn đề gì?

**Bài toán thực tế:** Ngân hàng có 41.188 bản ghi khách hàng. Họ cần biết: *"Khách nào sẽ đăng ký tiền gửi nếu gọi điện?"* — để không lãng phí cuộc gọi vào người sẽ từ chối.

**Vấn đề kỹ thuật:** Dữ liệu lớn, nhiều cột phức tạp, lớp dữ liệu mất cân bằng (~89% nói "không", ~11% nói "có"). Chạy Python thường trên laptop sẽ chậm, không tận dụng được nhiều CPU, và không scale được khi dữ liệu tăng lên triệu dòng.

**Giải pháp của dự án:**

| Vấn đề | Giải pháp |
|---|---|
| Python chạy 1 CPU, chậm | Apache Spark chạy song song nhiều CPU cùng lúc |
| Laptop hết RAM khi data lớn | Kubernetes phân phối tính toán ra nhiều máy |
| Model xong thì mất | PVC (ổ đĩa K8s) lưu model bền vững |
| Khó deploy lên server thật | Docker đóng gói → chạy được ở bất kỳ đâu |

> **Nói đơn giản:** Thay vì chạy `python train.py` trên laptop, dự án chạy Spark job trên một cụm Kubernetes — nhanh hơn, có thể scale, và model được lưu an toàn.

---

## Kiến trúc tổng quan

![Sơ đồ kiến trúc](docs/architecture.png)

Có 3 bước lớn:

```
1. Viết code (Colab)  →  2. Đóng gói (Docker)  →  3. Chạy (Kubernetes)
```

---

## Bước 1 — Viết code trên Google Colab

Trước tiên thử nghiệm code PySpark ở chế độ local (`local[*]`) trên Colab — chạy được ngay, không cần dựng server.  
Khi code ổn định mới chuyển lên Kubernetes.

---

## Bước 2 — Đóng gói bằng Docker

Kubernetes không chạy file `.py` trực tiếp. Nó chỉ chạy **container** (gói phần mềm hoàn chỉnh).

```
bank_job.py + Python + Spark  →  Docker build  →  image bank-spark:1.0
```

Image này chứa đủ mọi thứ cần thiết — copy sang máy khác vẫn chạy được.

---

## Bước 3 — Chạy trên Kubernetes

### Spark Operator — "quản đốc tự động"

Thay vì tự tay tạo từng pod, bạn chỉ cần viết một file `spark-app.yml` mô tả job cần chạy.  
**Spark Operator** đọc file đó và tự làm hết: tạo Driver, tạo Executor, dọn dẹp sau khi xong.

```
kubectl apply -f k8s/spark-app.yml
       ↓
Spark Operator đọc spec
       ↓
Tạo Driver pod  →  Driver tạo 2 Executor pod  →  Train model  →  Lưu vào PVC
```

> **Về chữ "Kubeflow" trong sơ đồ:** Spark Operator được bảo trì bởi tổ chức Kubeflow trên GitHub (`github.com/kubeflow/spark-operator`). Nhưng dự án này **không dùng Kubeflow** (nền tảng MLOps riêng). Chỉ dùng Spark Operator — một công cụ độc lập, chỉ liên quan đến Kubeflow ở chỗ cùng tổ chức.

### Driver và Executor — "não" và "tay chân"

- **Driver pod**: lên kế hoạch, chia việc, thu kết quả. Chỉ có 1.
- **Executor pod**: làm việc thực sự, chạy song song. Dự án này có 2.

```
Driver (não)  →  giao việc  →  Executor 1  ┐
                             →  Executor 2  ┘  → kết quả về Driver → lưu model
```

---

## 3 khái niệm hay gặp trong sơ đồ

### DAG — Kế hoạch thực thi của Spark

Khi bạn viết code Spark (`đọc CSV → lọc → encode → train → lưu`), Spark **không chạy ngay**.  
Nó xây một sơ đồ gọi là **DAG** (đồ thị bước không vòng lặp) rồi tối ưu trước khi thực sự tính toán.

```
đọc CSV → lọc cột → encode → train GBT → lưu model
   A    →    B    →    C   →     D     →      E
```

Mục đích: giảm đọc/ghi đĩa, ghép các bước nhỏ lại — chạy nhanh hơn.  
Bạn xem trực quan tại Spark UI `:4040` → tab **Stages**.

### SA — ServiceAccount (Tài khoản dịch vụ)

Trong Kubernetes, mọi pod đều cần **danh tính** để hệ thống biết pod đó được phép làm gì.  
**SA (ServiceAccount)** là danh tính đó.

Trong `k8s/spark-app.yml`:
```yaml
driver:
  serviceAccount: spark-operator-spark
```

Driver pod chạy với SA `spark-operator-spark` — được Spark Operator tạo tự động, có quyền:
- Tạo/xóa Executor pod
- Đọc cấu hình (ConfigMap)
- Báo cáo trạng thái job

Nếu sai SA → Driver không tạo được Executor → job thất bại với lỗi `Forbidden`.

### PVC — Ổ đĩa bền vững

Pod chết thì dữ liệu trong pod mất. **PVC (Persistent Volume Claim)** là ổ đĩa tồn tại độc lập với pod.

```
Driver pod ghi model vào PVC  →  Driver pod chết  →  PVC vẫn còn
                                                      ↓
                               Exporter pod đọc PVC  →  copy ra máy local
```

**PVC chỉ bị xóa khi bạn chủ động chạy** `kubectl delete pvc bank-models`.

---

## ML Pipeline — Dữ liệu đi qua những bước nào?

![Sơ đồ ML Pipeline](docs/pipeline.png)

**Giai đoạn 1 — Làm sạch dữ liệu:**
- Bỏ cột `duration` (thời gian cuộc gọi) — chỉ biết sau khi kết quả có rồi, dùng để train là gian lận dữ liệu.
- Cân bằng lớp: 89% "no" / 11% "yes" → model sẽ bỏ sót khách tiềm năng nếu không xử lý.

**Giai đoạn 2 — Train model:**
- Dùng **GBT** (Gradient-Boosted Trees) — mạnh với dữ liệu có cấu trúc kiểu bảng.
- **CrossValidator** tìm tham số tốt nhất qua 2-fold CV.
- Đánh giá bằng **AUC-PR** thay AUC-ROC vì dữ liệu mất cân bằng.

---

## Chạy thực tế

```bash
# 1. Build image
docker build -t bank-spark:1.0 .

# 2. Tạo ổ đĩa PVC
kubectl apply -f k8s/model-pvc.yml

# 3. Gửi Spark job
kubectl apply -f k8s/spark-app.yml

# 4. Theo dõi log Driver
kubectl logs -f -n spark-jobs $(kubectl get pods -n spark-jobs -o name | grep driver)

# 5. Xem Spark UI (DAG, Stages)
kubectl port-forward -n spark-jobs svc/bank-marketing-ui-svc 4040:4040

# 6. Copy model ra local sau khi job xong
kubectl apply -f k8s/model-exporter.yml
kubectl cp spark-jobs/bank-model-exporter:/models ./bank_gbt_best
```

---

## Cấu trúc thư mục

```
bank-marketing-spark-k8s/
├── src/bank_job.py              # Toàn bộ logic ML pipeline
├── k8s/
│   ├── spark-app.yml            # Gửi job lên K8s (entry point)
│   ├── model-pvc.yml            # Tạo ổ đĩa PVC
│   └── model-exporter.yml       # Copy model ra local
├── data/bank-additional-full.csv
├── Dockerfile
└── bank_gbt_best/               # Model output
```

---

## Từ điển nhanh

| Từ | Nghĩa đơn giản |
|---|---|
| **Pod** | Một "hộp" chạy code, đơn vị nhỏ nhất trong K8s |
| **Namespace** | Nhóm tách biệt tài nguyên K8s (như thư mục) |
| **CRD / SparkApplication** | K8s mở rộng thêm loại object mới — `SparkApplication` là ví dụ |
| **SA (ServiceAccount)** | Danh tính của pod, dùng để cấp quyền |
| **DAG** | Sơ đồ kế hoạch thực thi Spark, không có vòng lặp |
| **PVC** | Ổ đĩa bền vững, sống độc lập với pod |
| **Driver** | Pod "não" — điều phối toàn bộ Spark job |
| **Executor** | Pod "tay chân" — tính toán song song |
| **Spark Operator** | Controller tự động tạo Driver/Executor từ `SparkApplication` |
| **GBT** | Gradient-Boosted Trees — thuật toán ML dùng trong dự án |
| **AUC-PR** | Metric đánh giá model tốt hơn khi dữ liệu mất cân bằng |

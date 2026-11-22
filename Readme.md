# Bank Marketing — Spark trên Kubernetes

<p align="center">
  <img src="docs/logo.webp" alt="Kubernetes + Apache Spark" width="400"/>
</p>

> Dành cho người mới. Mục tiêu: hiểu **tại sao** từng thành phần tồn tại trước khi đọc code.

---

## Dự án làm gì?

Huấn luyện mô hình ML dự đoán khách hàng có đăng ký tiền gửi ngân hàng (`yes`/`no`) từ bộ dữ liệu **UCI Bank Marketing** (41.188 dòng, 21 cột).  
Toàn bộ tính toán chạy trên **Apache Spark** bên trong cụm **Kubernetes** — như "đám mây mini" ngay trên máy tính.

---

## Kiến trúc tổng quan

![Sơ đồ kiến trúc](docs/architecture.png)

Ba vùng chính:

| Vùng | Vai trò |
|---|---|
| **Google Colab** | Thử nghiệm code, train nhanh ở chế độ local |
| **Máy local** | Build Docker image, gửi manifest lên K8s |
| **Kubernetes cluster** | Chạy Spark job thực sự, lưu model vào PVC |

---

## Vùng 1 — Phát triển (Google Colab)

| Thành phần | Vai trò |
|---|---|
| `bank-additional-full.csv` | Dữ liệu gốc, dấu phân cách `;` |
| Notebook Colab | Viết thử PySpark 4.0.2 ở `local[*]` |
| `PipelineModel` | Model GBT tốt nhất, lưu tại `models/bank_gbt_best` |

---

## Vùng 2 — Đóng gói & Điều khiển

| Thành phần | Vai trò |
|---|---|
| `src/bank_job.py` | Script Python chính — đọc CSV, train, lưu model |
| `Dockerfile` | Đóng gói vào image `bank-spark:1.0` |
| `k8s/spark-app.yml` | Manifest `SparkApplication` gửi lên K8s |
| `kubectl apply` | Lệnh kích hoạt toàn bộ quá trình |
| Spark UI `:4040` | Xem tiến trình job qua `kubectl port-forward` |

**Tại sao phải Docker?** Kubernetes không chạy file `.py` trực tiếp — nó chỉ chạy container. Image chứa đủ Python, Spark, và code.

---

## Vùng 3 — Kubernetes Cluster

### 3.1 Spark Operator

```
kubectl apply spark-app.yml
        │
        ▼
kube-apiserver  ──watch──►  Spark Operator
                             └─► tạo Driver pod
```

**Spark Operator** là một controller chạy trong namespace `spark-operator`.  
Nó theo dõi object `SparkApplication` và tự động tạo Driver + Executor pod khi thấy spec mới.

> Giống "quản đốc": bạn giao việc qua `spark-app.yml`, nó tự sắp xếp nhân công.

---

### 3.2 Driver pod

```
Driver pod
├── Khởi tạo SparkSession
├── Đọc DAG (kế hoạch thực thi) từ bank_job.py
├── Giao task cho Executor
└── Thu kết quả, lưu model vào /models (PVC)
```

Driver là "não" — lên kế hoạch. Executor là "tay chân" — tính toán thực sự.

---

### 3.3 Executor pods (2 pod)

Mỗi Executor: **1 CPU, 1 GiB RAM**.  
Nhận partition dữ liệu từ Driver, xử lý song song, trả kết quả về. Khi job xong → pod tự xóa.

---

### 3.4 PVC `bank-models`

**PVC = Persistent Volume Claim** — ổ đĩa dùng chung bên trong cụm, **tồn tại độc lập với pod**.

- Driver mount `/models` → ghi `PipelineModel` sau khi train
- `bank-model-exporter` mount read-only → copy model ra máy local bằng `kubectl cp`

---

## ❓ Giải thích 3 khái niệm hay bị nhầm

### DAG — Directed Acyclic Graph

DAG là **sơ đồ thực thi** mà Spark tự xây dựng khi đọc code `bank_job.py`.

```
đọc CSV → lọc cột → encode → train GBT → ghi model
   A    →    B    →    C   →     D     →     E
```

Mỗi bước là một node, mũi tên là phụ thuộc — không có vòng lặp (acyclic).  
Spark tối ưu DAG này trước khi thực sự chạy, ghép các bước nhỏ lại để giảm đọc/ghi đĩa.  
Bạn xem DAG trực quan tại Spark UI `:4040` → tab **Stages**.

> **Dự án này KHÔNG dùng Kubeflow.** Kubeflow là nền tảng MLOps riêng trên K8s, có DAG pipeline riêng (Kubeflow Pipelines). Dự án này chỉ dùng Spark DAG Scheduler — built-in trong Spark.

---

### SA — ServiceAccount

SA = **ServiceAccount** trong Kubernetes — định danh cho pod, dùng để cấp quyền.

Trong `k8s/spark-app.yml`:
```yaml
driver:
  serviceAccount: spark-operator-spark
```

Driver pod chạy với SA `spark-operator-spark`. SA này được Spark Operator tạo tự động và cấp quyền:
- Tạo/xóa Executor pod
- Đọc ConfigMap
- Cập nhật trạng thái SparkApplication

Nếu không có SA đúng → Driver không thể tạo Executor → job thất bại với lỗi RBAC.

---

### PVC có chết khi Pod chết không?

**Không.** PVC tồn tại độc lập với pod.

```
Pod Driver   ──mount──►  PVC bank-models  (ghi model)
Pod chết ✗               PVC vẫn còn ✓
Pod Exporter ──mount──►  PVC bank-models  (đọc model)
```

Pod là tạm thời — sinh ra để làm việc rồi mất. PVC là bền vững — dữ liệu không bị mất khi pod xóa.  
PVC chỉ bị xóa khi bạn chủ động chạy `kubectl delete pvc bank-models`.

---

## ML Pipeline — 2 giai đoạn

![Sơ đồ ML Pipeline](docs/pipeline.png)

### Giai đoạn 1 — Chuẩn bị dữ liệu

- **Bỏ `duration`**: thời gian cuộc gọi chỉ biết sau khi có kết quả → data leakage.
- **Cân bằng lớp**: dữ liệu gốc ~89% "no" / ~11% "yes" → model sẽ bỏ sót khách tiềm năng nếu không xử lý.

### Giai đoạn 2 — Spark ML Pipeline

- **GBT** (Gradient-Boosted Trees): model ensemble mạnh với dữ liệu có cấu trúc.
- **CrossValidator**: tìm siêu tham số tốt nhất qua 2-fold CV.
- **AUC-PR thay AUC-ROC**: với dữ liệu mất cân bằng, AUC-PR phản ánh chính xác hơn khả năng phát hiện lớp thiểu số.

---

## Cấu trúc thư mục

```
bank-marketing-spark-k8s/
├── src/
│   └── bank_job.py              # Toàn bộ logic ML pipeline
├── k8s/
│   ├── spark-app.yml            # SparkApplication manifest (entry point K8s)
│   ├── model-pvc.yml            # Khai báo PVC bank-models (1 GiB)
│   └── model-exporter.yml       # Pod copy model ra local
├── data/
│   └── bank-additional-full.csv
├── Dockerfile                   # Build image bank-spark:1.0
├── requirements.txt
└── bank_gbt_best/               # Model đã train (output cục bộ)
    ├── model/
    └── metrics.json
```

---

## Chạy thực tế (step-by-step)

```bash
# 1. Build image
docker build -t bank-spark:1.0 .

# 2. Tạo ổ đĩa PVC
kubectl apply -f k8s/model-pvc.yml

# 3. Gửi Spark job
kubectl apply -f k8s/spark-app.yml

# 4. Theo dõi Driver pod
kubectl logs -f -n spark-jobs $(kubectl get pods -n spark-jobs -o name | grep driver)

# 5. Xem Spark UI
kubectl port-forward -n spark-jobs svc/bank-marketing-ui-svc 4040:4040

# 6. Copy model ra local sau khi job xong
kubectl apply -f k8s/model-exporter.yml
kubectl cp spark-jobs/bank-model-exporter:/models ./bank_gbt_best
```

---

## Bảng tra cứu nhanh

| Khái niệm | Giải thích |
|---|---|
| **Pod** | Đơn vị nhỏ nhất K8s, chứa 1+ container |
| **Namespace** | Nhóm logic tách biệt tài nguyên (như thư mục) |
| **CRD** | Custom Resource Definition — mở rộng API K8s, ví dụ `SparkApplication` |
| **SA (ServiceAccount)** | Định danh cho pod, dùng cấp quyền RBAC |
| **DAG** | Kế hoạch thực thi Spark — đồ thị bước không vòng lặp |
| **PVC** | Ổ đĩa bền vững, **sống độc lập** với pod |
| **Spark Driver** | Tiến trình điều phối job, tạo Executor |
| **Spark Executor** | Tiến trình tính toán song song |
| **Pipeline (MLlib)** | Chuỗi bước: fit một lần, transform nhiều lần |
| **CrossValidator** | Tìm siêu tham số tốt nhất qua k-fold CV |
| **AUC-PR** | Metric tốt hơn AUC-ROC khi dữ liệu mất cân bằng |
| **Spark Operator** | Controller K8s tự động tạo Driver/Executor từ `SparkApplication` |

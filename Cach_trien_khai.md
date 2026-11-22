# Chạy Bank Marketing PySpark trên Kubernetes

Làm theo các bước dưới đây bằng **PowerShell**. Hướng dẫn dùng Docker Desktop và Minikube trên Windows.

## Project tạo ra những gì?

Job đọc `data/bank-additional-full.csv`, huấn luyện các mô hình Spark ML và lưu kết quả vào:

```text
.\bank_gbt_best
├── model\          Model GBT tốt nhất
├── metrics.json    Số dòng dữ liệu và chỉ số AUC-PR, F1
├── predictions\    Dự đoán tập test dạng CSV
└── job.log         Log của lần chạy
```

Job đang dùng chế độ demo nhanh với mẫu phân tầng 10% dữ liệu. Kết quả phù hợp để chạy thử, không thay thế lần huấn luyện trên toàn bộ dữ liệu.

## 1. Cài phần mềm

Cần cài:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — mở Docker Desktop trước khi chạy.
- [Minikube](https://minikube.sigs.k8s.io/docs/start/) — chạy Kubernetes trên máy cá nhân.
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/) — gửi lệnh Kubernetes.
- [Helm 3](https://helm.sh/docs/intro/install/) — cài Spark Operator.

VS Code extensions **Kubernetes**, **Docker**, **Python**, **Pylance** là tùy chọn, không bắt buộc để chạy job.

Mở PowerShell mới và kiểm tra:

```powershell
docker version
minikube version
kubectl version --client
helm version
```

Nếu đã cài Minikube và Spark Operator, có thể bỏ qua các bước thiết lập đã hoàn tất.

## 2. Mở project và khởi động Minikube

Mở PowerShell tại thư mục project:

```powershell
cd C:\Users\Lenovo\Desktop\bank-marketing-spark-k8s
```

Kiểm tra dữ liệu đầu vào. Kết quả cần là `True`:

```powershell
Test-Path .\data\bank-additional-full.csv
```

Đảm bảo Docker Desktop đang chạy, rồi khởi động Minikube:

```powershell
minikube start --driver=docker --cpus=4 --memory=8g
kubectl get nodes
```

Node cần có trạng thái `Ready`. Nếu máy thiếu tài nguyên, giảm `--cpus` hoặc `--memory`.

## 3. Cài Spark Operator và tạo PVC

Tạo namespace cho job:

```powershell
kubectl create namespace spark-jobs --dry-run=client -o yaml | kubectl apply -f -
```

Cài Spark Operator:

```powershell
helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update
helm repo update
helm upgrade --install spark-operator spark-operator/spark-operator --version 2.6.0 --namespace spark-operator --create-namespace --set "spark.jobNamespaces={spark-jobs}" --wait
```

Tạo PVC để giữ kết quả sau khi job kết thúc:

```powershell
kubectl apply -f k8s\model-pvc.yml
kubectl get pods -n spark-operator
kubectl get pvc -n spark-jobs
```

PVC `bank-models` thường chuyển sang trạng thái `Bound`; Spark gắn PVC cho cả driver và executor để các stage lưu model có thể ghi dữ liệu. Không xóa PVC nếu muốn giữ model trong Kubernetes.

## 4. Build image của project

Chạy tại thư mục gốc project:

```powershell
docker build -t bank-spark:1.0 .
minikube image load bank-spark:1.0
```

Nếu sửa `src\bank_job.py` hoặc `Dockerfile`, phải chạy lại cả hai lệnh trên trước khi tạo job mới.

## 5. Chạy và theo dõi job

Nếu job cũ đang tồn tại, chỉ xóa khi job đã kết thúc hoặc bạn chủ động muốn hủy:

```powershell
kubectl delete sparkapp bank-marketing -n spark-jobs --ignore-not-found
```

Tạo job:

```powershell
kubectl apply -f k8s\spark-app.yml
kubectl get sparkapp bank-marketing -n spark-jobs -w
```

`Ctrl+C` chỉ dừng theo dõi, không dừng job. Khi trạng thái là `COMPLETED`, job đã xong.

Để xem log, mở **PowerShell thứ hai**:

```powershell
kubectl logs -f bank-marketing-driver -n spark-jobs
```

Nếu driver chưa xuất hiện, chờ vài giây rồi thử lại:

```powershell
kubectl get pods -n spark-jobs
```

## 6. Chép toàn bộ kết quả về Project

Thực hiện sau khi job đã `COMPLETED` và PowerShell đang ở thư mục gốc project (bước 2). Không chép từ driver pod đã kết thúc; dùng pod exporter. Lệnh `kubectl cp` tạo `.\bank_gbt_best` ngay trong project, không dùng `.\models`:

```powershell
kubectl apply -f k8s\model-exporter.yml
kubectl wait --for=condition=Ready pod/bank-model-exporter -n spark-jobs --timeout=120s
kubectl cp spark-jobs/bank-model-exporter:/models/bank_gbt_best .\bank_gbt_best
kubectl logs bank-marketing-driver -n spark-jobs | Set-Content -Encoding utf8 .\bank_gbt_best\job.log
kubectl delete -f k8s\model-exporter.yml
```

Kết quả nằm ngay tại thư mục gốc project trong `.\bank_gbt_best`. Xem file và chỉ số:

```powershell
Get-ChildItem .\bank_gbt_best -Recurse
Get-Content .\bank_gbt_best\metrics.json
```

Mở file `part-*.csv` trong `predictions` bằng Excel để xem dự đoán `actual` và `predicted`. Thư mục `model` là model Spark đã huấn luyện. Mỗi lần chạy mới sẽ thay kết quả cũ trên PVC; hãy đổi tên thư mục local nếu muốn giữ bản export trước đó.

## 7. Mở Spark UI tại localhost

Spark UI chỉ mở được khi driver pod đang `Running`. Kiểm tra:

```powershell
kubectl get pods -n spark-jobs
```

Trong một PowerShell riêng, chạy lệnh sau và **để cửa sổ đó mở**:

```powershell
kubectl port-forward -n spark-jobs pod/bank-marketing-driver 4040:4040
```

Mở trình duyệt và vào <http://localhost:4040>. Khi job đã kết thúc, Spark UI không còn hoạt động; đây là bình thường.

Nếu port-forward báo `failed to find sandbox` hoặc `lost connection to pod`, pod driver cũ đã bị thay thế hoặc khởi động lại. Kiểm tra pod hiện tại:

```powershell
kubectl get sparkapp bank-marketing -n spark-jobs
kubectl get pods -n spark-jobs
```

Khi `bank-marketing-driver` ở trạng thái `Running`, có thể chạy lại lệnh `kubectl port-forward` ở trên. Nếu số `ATTEMPTS` tiếp tục tăng, Spark job đang lỗi và Spark Operator tạo driver mới; port-forward sẽ lại mất kết nối. Mở PowerShell khác để lưu log trước khi pod bị thay thế:

```powershell
kubectl logs -f bank-marketing-driver -n spark-jobs | Tee-Object -FilePath .\bank-marketing-driver.log
```

Nếu pod đã `Completed`, Spark UI của lần chạy đó không còn mở được.

## 8. Lỗi thường gặp

| Lỗi                                                   | Cách xử lý                                                                                                    |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `minikube` hoặc `kubectl` không được nhận diện        | Cài công cụ tương ứng, mở PowerShell mới rồi thử lại.                                                         |
| Node không `Ready`                                    | Mở Docker Desktop, kiểm tra `minikube status`, rồi thử `minikube start --driver=docker --cpus=4 --memory=8g`. |
| Pod `ImagePullBackOff`                                | Chạy lại `docker build -t bank-spark:1.0 .` và `minikube image load bank-spark:1.0`.                          |
| Driver pod chưa tồn tại                               | Đợi job khởi động rồi xem `kubectl get pods -n spark-jobs`.                                                   |
| PVC chưa `Bound`                                      | Xem `kubectl get storageclass` và `kubectl describe pvc bank-models -n spark-jobs`.                           |
| `kubectl cp` báo pod đã `Completed`                   | Dùng pod exporter ở bước 6, không chép từ driver pod.                                                         |
| `port-forward` báo pod không tồn tại, `failed to find sandbox` hoặc `lost connection to pod` | Kiểm tra job và `ATTEMPTS`; nếu tăng, theo dõi log bằng `Tee-Object` ở bước 7. Nếu driver vẫn `Running`, thử lại port-forward. |
| Log có `Mkdirs failed to create /models/...` | PVC cần được mount cho cả driver và executor. Cấu hình hiện tại trong `k8s\spark-app.yml` đã mount cho cả hai. |
| Pod `OOMKilled`                                       | Tăng tài nguyên Docker Desktop/Minikube nếu máy đủ khả năng, rồi chạy lại job.                                |

## 9. Dừng hoặc xóa tài nguyên

Dừng Minikube nhưng giữ cluster và dữ liệu:

```powershell
minikube stop
```

Xóa toàn bộ Minikube cluster:

```powershell
minikube delete
```

Lệnh `minikube delete` cũng xóa dữ liệu PVC trong cluster. Các file đã export về `.\bank_gbt_best` trong project sẽ không bị xóa.

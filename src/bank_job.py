# src/bank_job.py
import json
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def main():
    print("=== KHỞI TẠO SPARK SESSION ===")
    spark = SparkSession.builder \
        .appName("BankMarketing-K8s-Batch") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 1. NẠP DỮ LIỆU
    data_path = "./data/bank-additional-full.csv"
    if not os.path.exists(data_path):
        # Fallback cho Kubernetes nếu image mount data ở /opt/spark/work-dir/data/
        data_path = "/opt/spark/work-dir/data/bank-additional-full.csv"

    print(f"-> Đang nạp dữ liệu từ: {data_path}")
    df = spark.read.csv(data_path, sep=";", header=True, inferSchema=True)
    total_data_rows = df.count()
    print(f"-> Số lượng bản ghi: {total_data_rows} | Số lượng cột: {len(df.columns)}")

    # 2. LÀM SẠCH VÀ TIỀN XỬ LÝ (ĐỔI TÊN CỘT CHỨA DẤU CHẤM)
    print("=== LÀM SẠCH VÀ TIỀN XỬ LÝ ===")
    # Đổi các cột chứa dấu chấm vĩ mô sang dấu gạch dưới
    df = df.withColumnRenamed("emp.var.rate", "emp_var_rate") \
           .withColumnRenamed("cons.price.idx", "cons_price_idx") \
           .withColumnRenamed("cons.conf.idx", "cons_conf_idx") \
           .withColumnRenamed("nr.employed", "nr_employed") \
           .dropDuplicates()

    # Xử lý rò rỉ dữ liệu: Bỏ cột duration
    df = df.drop("duration")

    # Xử lý pdays = 999
    df = df.withColumn("contacted_before", when(col("pdays") == 999, 0).otherwise(1)) \
           .withColumn("pdays_cleaned", when(col("pdays") == 999, -1).otherwise(col("pdays"))) \
           .drop("pdays")

    # Chuyển đổi nhãn y thành label nhị phân (0 và 1)
    df = df.withColumn("label", when(col("y") == "yes", 1.0).otherwise(0.0)).drop("y")

    # Lấy mẫu phân tầng để cấu hình local hoàn tất nhanh hơn.
    df = df.sampleBy("label", fractions={0.0: 0.1, 1.0: 0.1}, seed=42).cache()
    sample_rows = df.count()
    print(f"-> Lấy mẫu phân tầng 10%: {sample_rows} bản ghi")

    # Chia riêng từng lớp để giữ tỷ lệ nhãn tương tự ở tập train và test.
    train_parts = []
    test_parts = []
    for label_value in (0.0, 1.0):
        class_train, class_test = df.filter(col("label") == label_value).randomSplit(
            [0.8, 0.2], seed=42 + int(label_value)
        )
        train_parts.append(class_train)
        test_parts.append(class_test)

    train_df = train_parts[0].unionByName(train_parts[1]).cache()
    test_df = test_parts[0].unionByName(test_parts[1]).cache()

    # Tính toán Class Weight để giải quyết mất cân bằng
    total = train_df.count()
    pos = train_df.filter(col("label") == 1.0).count()
    neg = total - pos
    weight_neg = total / (2.0 * neg)
    weight_pos = total / (2.0 * pos)
    
    train_df = train_df.withColumn("weight", when(col("label") == 1.0, weight_pos).otherwise(weight_neg))
    train_rows = train_df.count()
    test_rows = test_df.count()
    print(f"-> Tập Train: {train_rows} (yes: {pos}, no: {neg})")
    print(f"-> Tập Test: {test_rows}")
    print(f"-> Trọng số lớp - yes (1): {weight_pos:.4f} | no (0): {weight_neg:.4f}")

    # 3. PHÁT TRIỂN ML PIPELINE
    print("=== XÂY DỰNG PIPELINE ===")
    categorical_cols = ["job", "marital", "education", "default", "housing", "loan", "contact", "month", "day_of_week", "poutcome"]
    numeric_cols = ["age", "campaign", "previous", "emp_var_rate", "cons_price_idx", "cons_conf_idx", "euribor3m", "nr_employed", "pdays_cleaned", "contacted_before"]

    # Mã hóa các biến phân loại
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_indexed", handleInvalid="keep") for c in categorical_cols]
    encoders = [OneHotEncoder(inputCol=f"{c}_indexed", outputCol=f"{c}_ohe") for c in categorical_cols]

    # Ghép tất cả thành một Vector đặc trưng duy nhất
    assembler_inputs = [f"{c}_ohe" for c in categorical_cols] + numeric_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)

    # 4. KHỞI TẠO CÁC MÔ HÌNH SO SÁNH
    lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight", maxIter=30)
    rf = RandomForestClassifier(featuresCol="features", labelCol="label", weightCol="weight", numTrees=10, maxDepth=4, seed=42)
    gbt = GBTClassifier(featuresCol="features", labelCol="label", maxDepth=3, maxIter=5, seed=42) # GBT MLlib không hỗ trợ trực tiếp weightCol bản 4.0

    models = {"Logistic Regression": lr, "Random Forest": rf, "Gradient-Boosted Trees": gbt}
    evaluator_auc = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderPR")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    evaluation_results = []

    for name, model in models.items():
        print(f"-> Đang huấn luyện và đánh giá: {name}...")
        pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, model])
        fitted_pipeline = pipeline.fit(train_df)
        predictions = fitted_pipeline.transform(test_df)
        
        # Đánh giá kết quả trên tập test
        auc_pr = evaluator_auc.evaluate(predictions)
        f1 = evaluator_f1.evaluate(predictions)
        print(f"   [KẾT QUẢ {name.upper()}] AUC-PR: {auc_pr:.4f} | F1-Score: {f1:.4f}")
        evaluation_results.append({
            "model": name,
            "auc_pr": float(auc_pr),
            "f1": float(f1),
        })

    # 5. Tinh chỉnh nhanh GBT bằng lưới tham số nhỏ và 2-fold CV.
    print("=== TINH CHỈNH GBT QUA CROSSVALIDATOR ===")
    gbt_pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, gbt])
    paramGrid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [3]) \
        .addGrid(gbt.maxIter, [3, 5]) \
        .build()

    cv = CrossValidator(estimator=gbt_pipeline,
                        estimatorParamMaps=paramGrid,
                        evaluator=evaluator_auc,
                        numFolds=2,
                        seed=42,
                        parallelism=2)

    print("-> Đang thực hiện chạy Cross-Validation nhanh (2-fold, 2 cấu hình) trên tập Train...")
    cv_model = cv.fit(train_df)
    
    # 6. ĐÁNH GIÁ MÔ HÌNH TỐT NHẤT
    best_predictions = cv_model.transform(test_df)
    best_auc_pr = evaluator_auc.evaluate(best_predictions)
    best_f1 = evaluator_f1.evaluate(best_predictions)
    print(f"=== KẾT QUẢ SAU TINH CHỈNH GBT ===")
    print(f"-> AUC-PR của mô hình tốt nhất trên tập Test: {best_auc_pr:.4f}")
    print(f"-> F1-Score của mô hình tốt nhất trên tập Test: {best_f1:.4f}")

    output_dir = "/models/bank_gbt_best"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    model_save_path = os.path.join(output_dir, "model")
    cv_model.bestModel.write().save(model_save_path)

    evaluation_results.append({
        "model": "Tuned Gradient-Boosted Trees",
        "auc_pr": float(best_auc_pr),
        "f1": float(best_f1),
    })
    metrics_path = os.path.join(output_dir, "metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as metrics_file:
        json.dump({
            "data": {
                "source": data_path,
                "total_rows": int(total_data_rows),
                "sample_rows": int(sample_rows),
                "train_rows": int(train_rows),
                "test_rows": int(test_rows),
                "sample_fraction": 0.1,
            },
            "evaluations": evaluation_results,
        }, metrics_file, ensure_ascii=False, indent=2)

    predictions_path = os.path.join(output_dir, "predictions")
    prediction_output = best_predictions \
        .withColumn("actual", when(col("label") == 1.0, "yes").otherwise("no")) \
        .withColumn("predicted", when(col("prediction") == 1.0, "yes").otherwise("no"))
    prediction_columns = categorical_cols + numeric_cols + ["actual", "predicted"]
    prediction_output.select(*prediction_columns) \
        .write.mode("overwrite").option("header", True).csv(predictions_path)

    print(f"-> Đã lưu model tại: {model_save_path}")
    print(f"-> Đã lưu metrics tại: {metrics_path}")
    print(f"-> Đã lưu dự đoán tại: {predictions_path}")

    spark.stop()
    print("=== HOÀN THÀNH JOB ===")

if __name__ == "__main__":
    main()
# 04_modeling.py
# Melatih Random Forest (Spark MLlib) untuk memprediksi PM2.5 harian,
# mengevaluasinya, dan menyiapkan data prediksi untuk dashboard.
import csv
import math
import os

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, functions as F

SEED = 42
BASE = "hdfs://localhost:9000/proyek_akhir/pm25"
IN = f"{BASE}/clean/pm25_clean.parquet"
MODEL_OUT = f"{BASE}/model/rf_pm25"
PRED_OUT = f"{BASE}/pred/pm25_prediksi.parquet"
DASH_CSV = "data/dashboard/pm25_prediksi.csv"

spark = (SparkSession.builder.master("local[*]").appName("modeling_pm25")
         .config("spark.driver.memory", "4g")
         .config("spark.hadoop.dfs.replication", "1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1. Baca data bersih dari HDFS dan bagi 80% latih / 20% uji (seed tetap)
df = spark.read.parquet(IN)
train, test = df.randomSplit([0.8, 0.2], seed=SEED)
train, test = train.cache(), test.cache()
print(f"Data latih: {train.count():,} baris | Data uji: {test.count():,} baris")

# 2. Pipeline: encoding wilayah -> gabung fitur -> Random Forest
fitur = ["latitude", "longitude", "month", "day_of_year", "state_idx"]
pipe = Pipeline(stages=[
    StringIndexer(inputCol="state_name", outputCol="state_idx", handleInvalid="keep"),
    VectorAssembler(inputCols=fitur, outputCol="features"),
    RandomForestRegressor(featuresCol="features", labelCol="pm25",
                          numTrees=100, maxDepth=10, maxBins=64, seed=SEED),
])
model = pipe.fit(train)
pred = model.transform(test).cache()


# 3. Evaluasi pada data uji
def metrik(nama):
    return RegressionEvaluator(labelCol="pm25", predictionCol="prediction",
                               metricName=nama).evaluate(pred)


rmse, mae, r2 = metrik("rmse"), metrik("mae"), metrik("r2")

# Pembanding sederhana: selalu menebak rata-rata data latih
rata2 = train.agg(F.avg("pm25")).first()[0]
rmse_base = math.sqrt(test.agg(F.avg((F.col("pm25") - rata2) ** 2)).first()[0])

print("\n== Evaluasi pada data uji ==")
print(f"RMSE : {rmse:.3f}")
print(f"MAE  : {mae:.3f}")
print(f"R2   : {r2:.3f}")
print(f"Pembanding (selalu menebak rata-rata latih = {rata2:.3f}): RMSE {rmse_base:.3f}")

# 4. Pentingnya tiap fitur
print("\n== Pentingnya tiap fitur ==")
rf = model.stages[-1]
for nama, nilai in sorted(zip(fitur, rf.featureImportances.toArray()), key=lambda x: -x[1]):
    print(f"{nama:12s} {nilai:.3f}")

# 5. Simpan model ke HDFS
model.write().overwrite().save(MODEL_OUT)
print("\nModel tersimpan di HDFS:", MODEL_OUT)

# 6. Data untuk dashboard: prediksi vs aktual pada data uji
hasil = (pred.select("date", "state_name", "latitude", "longitude", "month",
                     F.col("pm25").alias("pm25_aktual"),
                     F.col("prediction").alias("pm25_prediksi"))
             .withColumn("selisih", F.col("pm25_prediksi") - F.col("pm25_aktual")))
hasil.write.mode("overwrite").parquet(PRED_OUT)

os.makedirs("data/dashboard", exist_ok=True)
rows = hasil.orderBy("date").collect()
with open(DASH_CSV, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(hasil.columns)
    w.writerows([list(r) for r in rows])
print(f"Data dashboard: {DASH_CSV} ({len(rows):,} baris)")

spark.stop()

# 02_eksplorasi.py
# Mengenali data PM2.5 EPA yang sudah ada di HDFS (belum ada yang diubah).
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("eksplorasi_pm25")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

PATH = "hdfs://localhost:9000/proyek_akhir/pm25/raw/daily_88101_2025.csv"
df = spark.read.csv(PATH, header=True, inferSchema=True, escape='"')

print("== 1. Ukuran data ==")
print("Jumlah baris :", df.count())
print("Jumlah kolom :", len(df.columns))

print("\n== 2. Tipe tiap kolom ==")
df.printSchema()

print("\n== 3. Jumlah nilai kosong per kolom ==")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]) \
  .show(1, truncate=False, vertical=True)

print("\n== 4. Perkiraan jumlah nilai berbeda per kolom ==")
df.agg(*[F.approx_count_distinct(c).alias(c) for c in df.columns]) \
  .show(1, truncate=False, vertical=True)

print("\n== 5. Rentang tanggal ==")
df.agg(F.min("Date Local"), F.max("Date Local")).show()

print("== 6. Sebaran wilayah ==")
print("Jumlah stasiun       :", df.select("State Code", "County Code", "Site Num").distinct().count())
print("Jumlah negara bagian :", df.select("State Name").distinct().count())

print("\n== 7. Satuan PM2.5 ==")
df.select("Units of Measure").distinct().show(truncate=False)

print("== 8. Statistik kadar PM2.5 (Arithmetic Mean) ==")
df.select("Arithmetic Mean").describe().show()
print("Jumlah nilai minus:", df.filter(F.col("Arithmetic Mean") < 0).count())

print("\n== 9. Baris ganda ==")
key = ["State Code", "County Code", "Site Num", "Date Local"]
print("Kombinasi stasiun+tanggal yang muncul >1 kali:",
      df.groupBy(key).count().filter("count > 1").count())
print("Baris yang benar-benar identik (kelebihan):",
      df.count() - df.dropDuplicates().count())

spark.stop()

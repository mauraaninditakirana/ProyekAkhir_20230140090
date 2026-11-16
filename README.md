# Prediksi Kadar PM2.5 Harian dengan Apache Spark

Proyek ini memprediksi kadar PM2.5 harian di stasiun pemantauan udara menggunakan
Random Forest Regressor di Spark MLlib. Model memakai koordinat stasiun, negara
bagian, bulan, dan hari dalam tahun. Hasil prediksi pada data uji dibandingkan
dengan pengukuran aktual dan ditampilkan di Tableau.

## Sumber data

Data berasal dari US EPA AirData, dataset Daily Summary Data PM2.5 FRM/FEM Mass
(kode parameter 88101) tahun 2025. CSV mentah berisi 758.834 baris data dan
29 kolom, dengan ukuran sekitar 279 MB setelah diekstrak.

- [Unduh dataset EPA](https://aqs.epa.gov/aqsweb/airdata/daily_88101_2025.zip)
- [Halaman sumber EPA AirData](https://aqs.epa.gov/aqsweb/airdata/download_files.html)

## Lingkungan

- WSL / [nama dan versi distro Linux]
- Java [versi]
- Hadoop / HDFS [versi]
- Apache Spark / PySpark [versi]
- Python [versi]
- Tableau Public untuk visualisasi

## Cara menjalankan

Jalankan perintah dari folder utama repo. HDFS harus aktif dan dapat diakses
di `hdfs://localhost:9000`. Jika alamat HDFS berbeda, sesuaikan konfigurasi
pada skrip yang membaca dan menulis ke HDFS.

1. **Ingest data.** Unduh ZIP EPA, ekstrak `daily_88101_2025.csv`, lalu unggah
   CSV ke direktori HDFS `/proyek_akhir/pm25/raw`.
   [Tambahkan perintah ingest atau cara menjalankan skrip yang digunakan.]
2. **Preprocessing.** Jalankan [perintah skrip preprocessing]. Langkah ini
   membersihkan pengukuran dan merata-ratakan nilai per stasiun per hari.
   Hasil run yang didokumentasikan berisi 335.352 baris. Output yang dibaca
   tahap pemodelan adalah `/proyek_akhir/pm25/clean/pm25_clean.parquet`.
3. **Pemodelan, evaluasi, dan ekspor.** Jalankan:

   ```bash
   spark-submit code/04_modeling.py
   ```

   Skrip membagi data secara acak dengan rasio 80:20 dan seed 42, melatih
   Random Forest, lalu mengevaluasinya pada data uji. Skrip menyimpan model
   di HDFS pada `/proyek_akhir/pm25/model/rf_pm25`, prediksi Parquet pada
   `/proyek_akhir/pm25/pred/pm25_prediksi.parquet`, dan CSV lokal pada
   `data/dashboard/pm25_prediksi.csv`.
4. **Visualisasi.** Buka `data/dashboard/pm25_prediksi.csv` di Tableau.
   Kolom `selisih` dihitung sebagai `pm25_prediksi - pm25_aktual`.
   Dashboard menampilkan peta stasiun, scatter plot aktual vs prediksi,
   dan tren rata-rata bulanan.

## Hasil evaluasi

| Ukuran | Hasil |
| --- | ---: |
| Baris data latih | 268.736 |
| Baris data uji | 66.616 |
| RMSE model (µg/m³) | 4,716 |
| MAE model (µg/m³) | 2,828 |
| R² | 0,331 |
| RMSE baseline (µg/m³) | 5,764 |

Baseline memprediksi rata-rata PM2.5 data latih untuk setiap baris data uji.
Metrik di atas berasal dari pembagian acak data tahun 2025. Kemampuan model
untuk meramal periode mendatang memerlukan evaluasi berdasarkan urutan waktu.

## Struktur repo

- `code/` — kode pipeline. `04_modeling.py` menangani pelatihan, evaluasi,
  penyimpanan model, dan ekspor hasil prediksi.
- `docs/hasil_model.txt` — catatan hasil evaluasi model.
- `data/dashboard/` — folder output lokal yang dibuat skrip pemodelan.

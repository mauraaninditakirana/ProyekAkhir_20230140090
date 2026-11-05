#!/bin/bash
# 01_ingest_hdfs.sh
# Mengambil data PM2.5 harian 2025 dari US EPA, lalu menyimpannya ke HDFS (batch).
set -e
URL="https://aqs.epa.gov/aqsweb/airdata/daily_88101_2025.zip"
RAW_DIR="$HOME/ProyekAkhir_20230140090/data/raw"
HDFS_DIR="/proyek_akhir/pm25/raw"

mkdir -p "$RAW_DIR"
wget -U "Mozilla/5.0" -O "$RAW_DIR/daily_88101_2025.zip" "$URL"
unzip -o "$RAW_DIR/daily_88101_2025.zip" -d "$RAW_DIR"

echo "== Cek berkas lokal =="
ls -lh "$RAW_DIR/daily_88101_2025.csv"
wc -l "$RAW_DIR/daily_88101_2025.csv"

echo "== Simpan ke HDFS =="
hdfs dfs -mkdir -p "$HDFS_DIR"
hdfs dfs -put -f "$RAW_DIR/daily_88101_2025.csv" "$HDFS_DIR/"
hdfs dfs -ls -h "$HDFS_DIR"

# File: download_jars.py
import os
import requests

# Список JAR-файлов, которые Spark пытался качать в логах
JARS = [
    # Hadoop AWS connector
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar",
    # Postgres Driver
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar",
    # AWS SDK Bundle (самый тяжелый, ~180MB)
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.23.19/bundle-2.23.19.jar",
    # Dependencies
    "https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar",
    "https://repo1.maven.org/maven2/org/checkerframework/checker-qual/3.31.0/checker-qual-3.31.0.jar",
]

DEST_DIR_jar = "predownload/jars"
DEST_DIR_spark = "predownload"


def download_file(DEST_DIR, url):
    local_filename = url.split("/")[-1]
    path = os.path.join(DEST_DIR, local_filename)

    if os.path.exists(path):
        print(f"\n⏩ {local_filename} already exists.")
        return

    print(f"\n⬇️ Downloading {local_filename}...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"✅ Saved to {path}")


if __name__ == "__main__":
    os.makedirs(DEST_DIR_jar, exist_ok=True)
    print("🚀 Starting JAR download...")
    for url in JARS:
        download_file(DEST_DIR_jar, url)
    print("🏁 All JARs downloaded.\n")

    os.makedirs(DEST_DIR_spark, exist_ok=True)
    print("🚀 Starting SPARK download...")
    download_file(DEST_DIR_spark, "https://dlcdn.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz")
    print("🏁 ALL FILES DOWNLOADED")

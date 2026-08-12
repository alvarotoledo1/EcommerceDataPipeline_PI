# Entorno de ejecución del pipeline PySpark.
#
# Spark 4 corre sobre la JVM y necesita Java 17. En lugar de instalar un JDK y una
# versión adicional de Python en la máquina del desarrollador, todo el runtime vive acá.
# Python 3.12 porque es la versión soportada por PySpark 4.x.

FROM python:3.12-slim-bookworm

# openjdk-17-jre-headless: la JVM que ejecuta Spark.
# procps: Spark invoca `ps` al levantar los workers.
# curl: para descargar los jars del conector S3A.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OLIST_PROJECT_ROOT=/app

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# --- Conector S3A (MinIO) -------------------------------------------------
# Las versiones NO son libres: hadoop-aws tiene que coincidir con el Hadoop que trae
# PySpark (3.4.1, ver `ls site-packages/pyspark/jars`), y el SDK de AWS con el que
# espera ese hadoop-aws. Una combinación distinta falla en runtime con
# NoSuchMethodError o ClassNotFoundException, no al construir la imagen.
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_VERSION=2.24.6
ENV SPARK_JARS_DIR=/usr/local/lib/python3.12/site-packages/pyspark/jars

RUN curl -fsSL -o "${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_JARS_DIR}/aws-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"

# El código se monta como volumen (ver docker-compose.yml), así que no se copia acá:
# permite editar y volver a ejecutar sin reconstruir la imagen.

CMD ["bash"]

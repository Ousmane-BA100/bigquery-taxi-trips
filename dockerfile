# 🔧 Image de base avec Java (nécessaire pour Spark)
FROM openjdk:8-jdk-slim

# 📦 Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    curl \
    procps \
    build-essential \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 🔥 Installer Spark manuellement
COPY spark-3.5.3-bin-hadoop3.tgz /opt/spark.tgz
RUN tar -xvzf /opt/spark.tgz -C /opt && \
    mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark && \
    rm /opt/spark.tgz

# 🌱 Définir les variables d'environnement Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# 🐍 Installer les dépendances Python
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip3 install --upgrade pip && pip3 install -r requirements.txt

# 📂 Copier le code source
COPY . /app

# ✅ Définir un point d'entrée par défaut (modifiable avec docker-compose)
CMD ["bash"]

# Spark Project

## Overview
Este projeto utiliza Apache Spark 3.5.0 com Docker para processamento de dados.

## Configuração

### Versão do Spark
```bash
docker pull bitnami/spark:3.5.0
```

### Verificar imagens instaladas
```bash
docker images
```

## Execução

### Usando Bitnami Spark Container
1. **Iniciar container:**
```bash
docker run -d --name spark-container -v /home/nobru/documentos/spark-project/src/spark:/app -w /app bitnami/spark:3.5.0 tail -f /dev/null
```

2. **Verificar arquivos:**
```bash
docker exec spark-container ls -la /app
```

3. **Executar aplicação Spark:**
```bash
# Execução básica
docker exec spark-container spark-submit pr-3-app.py

# Com configuração de master local (2 cores)
docker exec spark-container spark-submit --master local[2] pr-3-app.py

# Com output verbose
docker exec spark-container spark-submit --verbose pr-3-app.py
```

### Usando Custom Docker Image
1. **Build da imagem:**
```bash
docker build -t my-spark-app:latest .
```

2. **Executar container:**
```bash
docker run -d --name my-spark-container my-spark-app:latest
```

3. **Verificar e executar:**
```bash
docker exec my-spark-container ls -la /app
docker exec my-spark-container spark-submit --master local[2] pr-3-app.py
```
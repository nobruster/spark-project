# spark-project

## versao do spark
docker pull bitnami/spark:3.5.0

docker images 

docker run -d --name spark-container -v /home/nobru/documentos/spark-project/src/spark:/app -w /app bitnami/spark:3.5.0 tail -f /dev/null

docker exec spark-container ls -la /app

docker exec spark-container spark-submit pr-3-app.py

docker exec spark-container spark-submit --master local[2] pr-3-app.py

docker exec spark-container spark-submit --verbose pr-3-app.py

docker build -t my-spark-app:latest .

docker run -d --name my-spark-container my-spark-app:latest

docker exec my-spark-container ls -la /app

docker exec my-spark-container spark-submit --master local[2] pr-3-app.py
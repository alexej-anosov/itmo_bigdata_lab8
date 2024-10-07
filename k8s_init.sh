kubectl create -f k8s/lab8_namespace.yaml

kubectl create -f k8s/lab8_clickhouse.yaml -n lab8

kubectl create -f k8s/lab8_sparkmaster.yaml -n lab8

kubectl create -f k8s/lab8_sparkworker.yaml -n lab8

kubectl port-forward svc/clickhouse-server -n lab8  9000:9000 &

kubectl port-forward svc/spark-master -n lab8 8081:8081 &

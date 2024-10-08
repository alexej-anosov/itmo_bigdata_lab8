minikube start --cpus=6 --memory=7192 --force

kubectl create -f k8s/lab8_namespace.yaml

kubectl create -f k8s/lab8_secrets.yaml -n lab8

kubectl create -f k8s/lab8_clickhouse.yaml -n lab8

kubectl create -f k8s/lab8_sparkmaster.yaml -n lab8

kubectl create -f k8s/lab8_sparkworker.yaml -n lab8


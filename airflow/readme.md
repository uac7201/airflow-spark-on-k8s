https://medium.com/@howdyservices9/executing-spark-operator-using-airflow-on-kubernetes-d5c17de7d376
https://medium.com/@howdyservices9/executing-spark-operator-using-airflow-on-kubernetes-part-2-62ae8fae7a7a

```
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace

helm show values apache-airflow/airflow > values.yaml

helm upgrade --install airflow apache-airflow/airflow `
  --namespace airflow --create-namespace `
  --values values.yaml


kubectl create clusterrolebinding default-admin --clusterrole cluster-admin --serviceaccount=airflow:airflow-worker --namespace spark-operator


kubectl port-forward svc/airflow-api-server 8080:8080 --namespace airflow
```

Github sync:
```
ssh-keygen -t rsa -b 4096 -C "maik.bialas@googlemail.com"

cat C:\Users\maikb/.ssh/id_rsa.pub

Github repo -> Settings -> Deploy keys -> Add deploy key ->
Title: airflow-git-ssh
Key: copy past content from the above file id_rsa.pub



kubectl create secret generic airflow-git-ssh-secret `
  --from-file=gitSshKey=C:\Users\maikb/.ssh/id_rsa `
  --from-file=known_hosts=C:\Users\maikb/.ssh/known_hosts `
  --from-file=id_ed25519.pub=C:\Users\maikb/.ssh/id_rsa.pub `
  -n airflow
```


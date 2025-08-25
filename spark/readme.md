```
helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator


helm install spark-operator spark-operator/spark-operator `
    --namespace spark-operator `
    --create-namespace `
    --wait


helm show values spark-operator/spark-operator > values.yaml


helm upgrade --install spark-operator spark-operator/spark-operator `
  --namespace spark-operator --create-namespace `
  --values values.yaml


```
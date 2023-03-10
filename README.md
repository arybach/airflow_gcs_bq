// use proper service account credentials
$ gcloud auth list
$ gcloud config set account airflow...

curl -sSL https://raw.githubusercontent.com/bitnami/containers/main/bitnami/airflow/docker-compose.yml > docker-compose.yml
docker-compose up -d

// default: -U user -P bitnami
mkdir -p ./dags ./logs ./plugins ./sources
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
sudo chown -R 1001:1001 ./dags
sudo chown -R $AIRFLOW_UID:$AIRFLOW_GID /airflow

// key-file.json will be needed to update google_cloud_conn in
// set in .env file if needed
_AIRFLOW_WWW_USER_USERNAME=...
_AIRFLOW_WWW_USER_PASSWORD=...
AIRFLOW_PROJ_DIR=/home/groot/gcpzoomcamp/airflow

pip install apache-airflow[gcp] 
pip install apache-airflow[google]
pip install apache-airflow-providers-google
pip install google.cloud 

// do not use sudo
docker-compose up
// change in requirements.txt -> docker-compose restart
docker-compose down --volumes --rmi all

// to check ids - password bitnami1 by default
docker exec -ti airflow_postgresql_1 psql -U bn_airflow -d bitnami_airflow -c 'SELECT task_id, dag_id, run_id, job_id, pid, external_executor_id, trigger_id FROM task_instance;'


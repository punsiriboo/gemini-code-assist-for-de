# Gemini Code Asisst for Data Engineering
# Data Pipelines with Apache Airflow
![Gemini-for-DE](images/topic_header.png)

เรียนรู้การสร้าง ใช้ Gemini Code Assit ในการสร้าง Data Pipelines โดยใช้ Apache Airflow ตั้งแต่อ่านข้อมูล ทำความสะอาดข้อมูล
และโหลดข้อมูลเข้า Data Lake/Data Warehouse อัตโนมัติ เพื่อนำไปวิเคราะห์ข้อมูลต่อไป โดยใช้  BigQueruy Data Canvas ในการช่วยสร้าง Data Visualization

## Files/Folders

| Name | Description |
| - | - |
| `dags/` | โฟลเดอร์ที่เก็บโค้ด DAG หรือ Data Pipelines ที่เราสร้างขึ้น |
| `plugins/` | โฟลเดอร์ที่เก็บ Plugins ของ Airflow |
| `cred/` | โฟลเดอร์ที่เก็บไฟล์ Configuration อย่างไฟล์ `airflow_local_settings.py` |
| `tests/` | โฟลเดอร์ที่เก็บ Tests |
| `docker-compose.yaml` | ไฟล์ Docker Compose ที่ใช้รัน Airflow ขึ้นมาบนเครื่อง |
|`prompts/`| โฟลเดอร์ที่เก็บ prompts ที่ใช้ในการ Generate Code หรือ Query

## Starting Airflow

Before we run Airflow, let's create these folders first:

```sh
mkdir -p ./dags ./config ./logs ./plugins ./tests
```

On **Linux**, please make sure to configure the Airflow user for the docker-compose:

```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

For Google Cloud Service accoun, and please save your service account file as sa.json in the `cred/` folder.
```sh
```
midir -p ./cred 
```

```sh
docker-compose up --build -d
```


Reference:
https://www.mongodb.com/developer/products/mongodb/mongodb-apache-airflow/
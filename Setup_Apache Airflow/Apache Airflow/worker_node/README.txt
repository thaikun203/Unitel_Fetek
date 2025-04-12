########## Khởi tạo Container lần đầu ##########
### Trên Worker Node (10.201.214.42)
[Có thể giảm số lượng Worker bằng cách giảm số lượng ở tham số replicas trong file docker-compose-worker.yml]
docker compose -f docker-compose-worker.yml up -d


########## Khởi động Container khi Upgrade Airflow Image ##########
B1: Down Celery Worker
docker compose -f docker-compose-worker.yml down

B2: Khởi động lại Celery Worker
docker compose -f docker-compose-worker.yml up -d

########## Khởi tạo Container lần đầu ##########
### Trên Master Node (10.201.214.41)
B1: Khởi tạo db cho airflow
docker compose -f docker-compose-init.yml up 

B2: Khởi tạo các Service cần thiết cho airflow celery executor
docker compose -f docker-compose.yml up -d 

[Có thể giảm số lượng Worker bằng cách loại bỏ các Service trong file docker-compose-worker.yml]
docker compose -f docker-compose-worker.yml up -d

B3: [Optional] Khởi động Celery Flower để giám sát Celery Worker
docker compose -f docker-compose-flower.yml up -d


########## Khởi động Container khi Upgrade Airflow Image ##########
B1: Down hết các Service của Airflow
docker compose -f docker-compose.yml -f docker-compose-worker.yml -f docker-compose-flower.yml down 

B2: Khởi động lại các Service
docker compose -f docker-compose.yml -f docker-compose-worker.yml -f docker-compose-flower.yml up -d
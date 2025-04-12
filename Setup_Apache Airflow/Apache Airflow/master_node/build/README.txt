########## Khởi tạo Image lần đầu ##########

### Tại máy Host
B1: Build mới Image 
docker build -t Mytel/airflow:2.10.3 -f Dockerfile .

B2: Save Image thành file zip
docker save -o airflow.zip Mytel/airflow:2.10.3

B3: sftp file Image.zip qua Master Node (10.201.214.41)


### Tại Master Node (10.201.214.41)
B4: Đọc Image từ file zip
docker load -i airflow.zip

B5: Bây giờ trên Master Node đã có thể sử dụng được Image Mytel/airflow:2.10.3


########## Upgrade Image ##########
### Tại máy Host
B1: Bổ sung thư viện python cần cài đặt vào file ./libs/requirements-local.txt

B2: Build mới Image 
docker build -t Mytel/airflow:2.10.3 -f Dockerfile .

B3: Save Image thành file zip
docker save -o airflow.zip Mytel/airflow:2.10.3

B4: sftp file Image.zip qua Master Node (10.201.214.41)


### Tại Master Node (10.201.214.41)
B5: Đọc Image từ file zip
docker load -i airflow.zip

B6: Bây giờ trên Master Node đã có thể sử dụng được Image Mytel/airflow:2.10.3 được Update


########## Update thêm các gói RPM, Package khác ở bên ngoài (Oracle Instant Client) ##########
Cần sửa lại nội dung trong Dockerfile. Sau đó build lại giống với bước Upgrade Image
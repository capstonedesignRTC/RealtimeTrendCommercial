sudo sudo yum update -y

sudo yum install git-core -y

git clone -b feature/spark --single-branch https://github.com/capstonedesignRTC/RealtimeTrendCommercial.git

cd RealtimeTrendCommercial/spark/utils

touch my_secret.py

echo profile_info = {\ 	
    \"aws_access_key_id\": \"\" ,\
    \"aws_secret_access_key\": \"\" ,\
    \"region\": \"\",\
    \"aws_bucket_name\": \"\" ,\  
    } >> my_secret.py 

cat my_secret.py

cd ../

pip3 install -r requirements.txt

spark-submit main.py
mkdir -p logs/
echo 'created logs folder'

echo 'checking compatibility' >> logs/installation.txt
sudo docker run --rm "debian:bookworm-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))' >> logs/installation.txt
echo -e '\n\n\n' >> logs/installation.txt

echo 'creating folders for airflow' >> logs/installation.txt
mkdir -p ./dags ./logs ./plugins ./config
echo -e '\n\n\n' >> logs/installation.txt

echo 'saving airflow user_id in environment veriable file' >> logs/installation.txt
echo -e "AIRFLOW_UID=$(id -u)" > .env
echo -e '\n\n\n' >> logs/installation.txt

sudo docker compose run airflow-cli airflow config list >> logs/installation.txt
echo -e '\n\n\n' >> logs/installation.txt

echo 'changing user access to files in config folder' >> logs/installation.txt
chmod -R 775 ./config || sudo chmod -R 775 ./config
echo -e '\n\n\n' >> logs/installation.txt

echo 'initializing airflow instance' >> logs/installation.txt
sudo docker compose up airflow-init >> logs/installation.txt
echo -e '\n\n\n' >> logs/installation.txt

echo 'removing orphans' >> logs/installation.txt
sudo docker compose down --volumes --remove-orphans

echo 'spinning up airflow instance'
sudo docker compose up -d
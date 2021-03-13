echo "======install virtualenv======"
pip install virtualenv

echo "======Create Virtual Environment======"
VIRTUAL_ENV_NAME=".data_modeling_pg_venv"
virtualenv -p python3.7 $VIRTUAL_ENV_NAME

echo "======Activate Virtual Environment======"
source $VIRTUAL_ENV_NAME/bin/activate

echo "======Install requirements======"
pip3 install -r requirements.txt

python create_tables.py

python etl.py

echo "======deactivate virtual environment ======"
deactivate

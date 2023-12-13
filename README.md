# Run AWS Athena Queries

This project demonstrates how to query from AWS Athena using Python scripts.

This script finds SQL statements in the `queries` folder, run the SQL query against Athena, and save returned result in `result` folder with same file name prepended by account id and date.

1. Create a Python environment and install required libraries.

```
python3 -m virtualenv --python=python3.11 .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Run the script.

```
python main.py
```

3. Check the result in `result` folder.

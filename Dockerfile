FROM python:3.8
WORKDIR .
COPY requirements.txt .
RUN python -m pip install -r requirements.txt
COPY *.py .
CMD ["python","./process_json.py"]
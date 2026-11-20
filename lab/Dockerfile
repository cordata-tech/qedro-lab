# The emitters, qedro, and the scripts that drive them.
FROM python:3.12-slim-trixie

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /lab
ENTRYPOINT ["python", "/lab/lab.py"]

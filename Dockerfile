FROM python AS server

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/shoots_server.py .

EXPOSE 8081
CMD ["python", "shoots_server.py","--host=0.0.0.0"]


FROM server AS test
COPY requirements-test.txt .
RUN pip install --no-cache-dir -r requirements-test.txt
COPY src/shoots_client.py . 
COPY src/test.py .
CMD ["python", "test.py"]

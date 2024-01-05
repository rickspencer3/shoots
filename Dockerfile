FROM python

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/shoots_server.py .

EXPOSE 8081
CMD ["python", "shoots_server.py","--host=0.0.0.0"]

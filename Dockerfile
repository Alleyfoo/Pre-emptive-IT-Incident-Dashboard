FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt google-cloud-storage

COPY . .

ENV PYTHONPATH=/app
ENV PORT=8080

EXPOSE 8080

CMD ["sh", "-c", "streamlit run streamlit_app.py --server.port=${PORT} --server.address=0.0.0.0"]

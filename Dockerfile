FROM python:3.11-slim

WORKDIR /app

COPY demos/requirements-demo.txt ./demos/requirements-demo.txt
RUN pip install --no-cache-dir -r demos/requirements-demo.txt

COPY . .

ENV PYTHONPATH=/app
ENV PORT=8080

EXPOSE 8080

CMD ["sh", "-c", "streamlit run demos/streamlit_app.py --server.port=${PORT} --server.address=0.0.0.0"]

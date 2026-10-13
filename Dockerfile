FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/
# CPU-only torch keeps the image small (sentence-transformers would otherwise pull CUDA builds)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
EXPOSE 8000
CMD ["uvicorn", "app.full_main:app", "--host", "0.0.0.0", "--port", "8000"]

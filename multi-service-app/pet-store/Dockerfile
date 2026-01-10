FROM python:3.11-slim

WORKDIR /app

# Update CA certificates to fix SSL verification issues
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pet-store/app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY pet-store/app/pet_store.py /app/pet_store.py
RUN mkdir -p /app/images

EXPOSE 5001

CMD ["python", "pet_store.py"]


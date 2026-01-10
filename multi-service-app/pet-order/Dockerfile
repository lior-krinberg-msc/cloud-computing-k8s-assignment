FROM python:3.11-slim

WORKDIR /app

# Docker context is multi-service-app
COPY pet-order/app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY pet-order/app/pet_order.py /app/pet_order.py

EXPOSE 5003

CMD ["python", "pet_order.py"]
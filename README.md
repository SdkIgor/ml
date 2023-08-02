# Как запустить сервис

```
cp .env.example .env # отредактируйте если необходимо
```

## Далее, если локально:

```
source .env
pip install -r requirements.txt
python3 flask_app.py
```

## На проде:

```
docker-compose up -d
```

# Переменные окружения

`MYGURU_CRM_DB_DSN` - dsn подключения к БД myguru

`MYGURU_AIMYLOGIC_TOKEN` - token для подключения к телефонии Aimylogic

`MYGURU_MONEY_FUSE` - "предохранитель" от случайной потери денег. Запрещает реальные звонки, разрешены только тестовые
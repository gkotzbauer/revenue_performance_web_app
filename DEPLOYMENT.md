# Deployment Instructions

## Backend
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

## Frontend
```bash
cd frontend
npm install
npm start
```

## Docker (Optional)
```bash
docker-compose up --build
```

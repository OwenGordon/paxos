from fastapi import FastAPI, Depends, Response
from fastapi.middleware.cors import CORSMiddleware

from uuid import uuid4
from pydantic import BaseModel

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker

# Create an in-memory database engine
engine = create_engine("sqlite:///:memory:", echo=True, connect_args={"check_same_thread": False})

# Create a sessionmaker factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a FastAPI app
app = FastAPI()
# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],  # List the origins that should be allowed, or use ["*"] for open access
    allow_credentials=True,
    allow_methods=["*"],  # Specify which methods can be used, ["GET", "POST"] etc.
    allow_headers=["*"],  # Specify which headers can be included
)

# Create a dependency to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Define a base model
class Log(BaseModel):
    timestamp: str
    sender: str
    receiver: str
    action: str
    message_id: str


Base = declarative_base()
class DBItem(Base):
    __tablename__ = 'items'
    id = Column(String, primary_key=True, index=True)
    timestamp = Column(String, index=True)
    sender = Column(String, index=True)
    receiver = Column(String, index=True)
    action = Column(String, index=True)
    message_id = Column(String, index=True)
Base.metadata.create_all(bind=engine)

class LogResponse(Log):
    id: str

# Create an endpoint to create an item
@app.post("/log/")
async def create_log(log: Log, db = Depends(get_db)):
    id = str(uuid4())
    dbitem: DBItem = DBItem(timestamp=log.timestamp, sender=log.sender, receiver=log.receiver, action=log.action, id=id, message_id=log.message_id)
    db.add(dbitem)
    db.commit()
    db.refresh(dbitem)
    return Response(status_code=200, content=f"Created item with id: {id}")

# Create an endpoint to get all items
@app.get("/logs/", response_model=list[LogResponse])
async def read_items(db = Depends(get_db)):
    raw_items: list[DBItem] = db.query(DBItem).all()
    logs = [LogResponse(timestamp=item.timestamp, sender=item.sender, receiver=item.receiver, action=item.action, message_id=item.message_id, id=item.id) for item in raw_items]
    return logs

@app.delete("/logs/")
async def delete_logs(db = Depends(get_db)):
    raw_items: list[DBItem] = db.query(DBItem).all()
    for item in raw_items:
        db.delete(item)
    db.commit()
    return {"message": "Logs deleted successfully"}
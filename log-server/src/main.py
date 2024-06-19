from fastapi import FastAPI, Depends, Response
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
    source: str
    destination: str
    action: str


Base = declarative_base()
class DBItem(Base):
    __tablename__ = 'items'
    id = Column(String, primary_key=True, index=True)
    timestamp = Column(String, index=True)
    source = Column(String, index=True)
    destination = Column(String, index=True)
    action = Column(String, index=True)
Base.metadata.create_all(bind=engine)

class LogResponse(Log):
    id: str

# Create an endpoint to create an item
@app.post("/log/")
async def create_log(log: Log, db = Depends(get_db)):
    id = str(uuid4())
    dbitem: DBItem = DBItem(timestamp=log.timestamp, source=log.source, destination=log.destination, action=log.action, id=id)
    db.add(dbitem)
    db.commit()
    db.refresh(dbitem)
    return Response(status_code=200, content=f"Created item with id: {id}")

# Create an endpoint to get all items
@app.get("/logs/", response_model=list[LogResponse])
async def read_items(db = Depends(get_db)):
    raw_items: list[DBItem] = db.query(DBItem).all()
    logs = [LogResponse(timestamp=item.timestamp, source=item.source, action=item.action, destination=item.destination, id=item.id) for item in raw_items]
    return logs

@app.delete("/logs/")
async def delete_logs(db = Depends(get_db)):
    raw_items: list[DBItem] = db.query(DBItem).all()
    for item in raw_items:
        db.delete(item)
    db.commit()
    return {"message": "Logs deleted successfully"}
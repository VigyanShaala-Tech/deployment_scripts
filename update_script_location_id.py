from sqlalchemy import create_engine, func, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv("config.env")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLAlchemy setup
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Table models
class StudentDetails(Base):
    __tablename__ = "student_details"
    __table_args__ = {"schema": "intermediate"}
    id = Column(Integer, primary_key=True)
    location_id = Column(Integer)

class GeneralInformationSheet(Base):
    __tablename__ = "general_information_sheet"
    __table_args__ = {"schema": "raw"}
    Student_id = Column(Integer, primary_key=True)
    State_Union_Territory = Column(String)
    District = Column(String)

class LocationMapping(Base):
    __tablename__ = "location_mapping"
    __table_args__ = {"schema": "intermediate"}
    location_id = Column(Integer, primary_key=True)
    state_union_territory = Column(String)
    district = Column(String)

# Step 1: Get students with NULL location_id and find matching location_id
matches = (
    session.query(
        StudentDetails.id,
        LocationMapping.location_id.label("location_id")
    )
    .join(
        GeneralInformationSheet,
        StudentDetails.id == GeneralInformationSheet.Student_id
    )
    .join(
        LocationMapping,
        func.lower(func.trim(GeneralInformationSheet.State_Union_Territory)) == func.lower(func.trim(LocationMapping.state_union_territory)),
    )
    .filter(
        func.lower(func.trim(GeneralInformationSheet.District)) == func.lower(func.trim(LocationMapping.district)),
        StudentDetails.location_id.is_(None)
    )
    .all()
)

# Step 2: Perform updates
updated_count = 0
for student_id, location_id in matches:
    session.query(StudentDetails).filter_by(id=student_id).update(
        {"location_id": location_id},
        synchronize_session=False
    )
    updated_count += 1

session.commit()
print(f"\n Successfully updated location_id for {updated_count} students using intermediate.location_mapping.")

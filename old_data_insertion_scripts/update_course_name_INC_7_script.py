import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

Base = declarative_base()

# Load DB credentials
load_dotenv(r'config.env') 


class GeneralInformationSheet(Base):
    __tablename__ = "general_information_sheet"
    __table_args__ = {"schema": "raw"}

    Student_id = Column(Integer, primary_key=True)
    Email = Column(String)
    Incubator_Batch = Column(String)
    Incubator_Course_Name = Column(String)


# Create engine using environment variables
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

Session = sessionmaker(bind=engine)
session = Session()


csv_path = r"C:\Users\vigya\Downloads\load_csv_to_db\GIS_sheet\cleaned_data_incubators_sheet.csv"  # contains columns: Student_id, Incubator_Course_Name
df = pd.read_csv(csv_path,)


df = df[df["Incubator_Batch"] == "Incubator 7.0"]


update_mappings = [
    {
        "Student_id": int(row.Student_id),
        "Incubator_Course_Name": row.Incubator_Course_Name
    }
    for row in df.itertuples(index=False)
]


if update_mappings:
    session.bulk_update_mappings(GeneralInformationSheet, update_mappings)
    session.commit()
    print(f"Updated {len(update_mappings)} rows for Incubator 7.0")
else:
    print("No matching rows for Incubator 7.0 found in CSV.")

session.close()

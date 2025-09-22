from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import time
import os
from dotenv import load_dotenv

# Load DB credentials
load_dotenv(r'config.env') 

# SQLAlchemy setup
Base = declarative_base()

class FilteredStudentData(Base):
    __tablename__ = 'general_information_sheet'
    __table_args__ = {'schema': 'raw'}

    Student_id = Column(Integer, primary_key=True)
    Incubator_Batch = Column(String)
    Currently_Pursuing_Degree = Column(String)
    Currently_Pursuing_Year = Column(String)
    Subject_Area = Column(String)
    Name_of_College_University = Column(String)
    University = Column(String)
    Country = Column(String)
    State_Union_Territory = Column(String)
    District = Column(String)
    City_Category = Column(String)
    Caste_Category = Column(String)
    Annual_Family_Income = Column(String)
    Assigned_Through = Column(String)
    College_Category = Column(String)
    Student_Role = Column(String)
    College_State_Union_Territory = Column(String)

# CSV and batch config
CSV_FILE_PATH = r"C:\Users\vigya\Downloads\load_csv_to_db\GIS_sheet\cleaned_data_incubators_sheet.csv"  
ALLOWED_BATCHES = ['Incubator 6.0']   #change incubators here    

# Read and filter CSV
df = pd.read_csv(CSV_FILE_PATH)
df = df[df['Incubator_Batch'].isin(ALLOWED_BATCHES)]

# Define columns to update
TARGET_COLUMNS = [
    "Currently_Pursuing_Degree", "Currently_Pursuing_Year", "Subject_Area",
    "Name_of_College_University", "University", "Country", "State_Union_Territory",
    "District", "City_Category", "Caste_Category", "Annual_Family_Income",
    "Assigned_Through", "College_Category", "Student_Role", "College_State_Union_Territory",
    "Incubator_Batch"
]

# Create engine using environment variables
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

Session = sessionmaker(bind=engine)
session = Session()

# Track time
start_time = time.time()

# Build mappings for bulk update
update_data = []
for _, row in df.iterrows():
    row_data = {col: row[col] for col in TARGET_COLUMNS if pd.notna(row[col])}
    row_data['Student_id'] = row['Student_id']
    update_data.append(row_data)

# Perform bulk update
session.bulk_update_mappings(FilteredStudentData, update_data)
session.commit()
session.close()

end_time = time.time()
print(f"* Bulk update completed: {len(update_data)} rows updated.")
print(f"* Time taken: {end_time - start_time:.2f} seconds")
import os
from sqlalchemy import create_engine, Column, Integer, String, func, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import update
from dotenv import load_dotenv
from sqlalchemy import case
from sqlalchemy import or_, func, update, case

# Load credentials from config.env
load_dotenv("config.env")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

Base = declarative_base()

class GeneralInformationSheet(Base):
    __tablename__ = "general_information_sheet"
    __table_args__ = {"schema": "raw"}
    Student_id = Column(Integer, primary_key=True)
    Incubator_Batch = Column(String)
    State_Union_Territory = Column(String)
    District = Column(String)

# Session setup
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Step 1: Detect states with extra spaces
state_rows = (
    session.query(
        GeneralInformationSheet.State_Union_Territory,
        func.trim(GeneralInformationSheet.State_Union_Territory).label("cleaned")
    )
    .filter(
        GeneralInformationSheet.Incubator_Batch.in_(
            ["Incubator 4.0", "TS/TT 1.0", "Incubator 6.0", "Incubator 7.0"]
        ),
        GeneralInformationSheet.State_Union_Territory != func.trim(GeneralInformationSheet.State_Union_Territory)
    )
    .distinct()
    .all()
)

# Step 2: Detect districts with extra spaces
district_rows = (
    session.query(
        GeneralInformationSheet.District,
        func.trim(GeneralInformationSheet.District).label("cleaned")
    )
    .filter(
        GeneralInformationSheet.Incubator_Batch.in_(
            ["Incubator 4.0", "TS/TT 1.0", "Incubator 6.0", "Incubator 7.0"]
        ),
        GeneralInformationSheet.District != func.trim(GeneralInformationSheet.District)
    )
    .distinct()
    .all()
)

print("👉 States that will be updated:")
for original, cleaned in state_rows:
    print(f'"{original}"  -->  "{cleaned}"')

print("\n👉 Districts that will be updated:")
for original, cleaned in district_rows:
    print(f'"{original}"  -->  "{cleaned}"')

# Step 3: Apply updates
session.query(GeneralInformationSheet).filter(
    GeneralInformationSheet.Incubator_Batch.in_(
        ["Incubator 4.0", "TS/TT 1.0", "Incubator 6.0", "Incubator 7.0"]
    )
).update(
    {
        GeneralInformationSheet.State_Union_Territory: func.trim(GeneralInformationSheet.State_Union_Territory),
        GeneralInformationSheet.District: func.trim(GeneralInformationSheet.District)
    },
    synchronize_session=False,
)

session.commit()
print("\n✅ Update completed successfully!")

stmt = (
    update(GeneralInformationSheet)
    .where(func.trim(GeneralInformationSheet.State_Union_Territory) == "New Delhi")
    .values(State_Union_Territory="Delhi")
)

result = session.execute(stmt)
session.commit()
print(f"\n✅ Replaced 'New Delhi' with 'Delhi' in {result.rowcount} rows.")

# Step 3: Replace specific District names
district_updates = {
    "Utnoor": "Adilabad",
    "Shadnagar": "Ranga Reddy",
    "Devarakonda": "Nalgonda",
    "Armoor": "Nizamabad",
    "Bhupalpally": "Jayashankar Bhupalapally",
    "Suryapeta": "Suryapet",
    "Asifabad": "Kumuram Bheem Asifabad",
    "Sircilla": "Rajanna Sircilla",
    "Kothagudem": "Bhadradri Kothagudem",
    "Janagaon": "Jangaon",
    "Jangoan": "Jangaon",
    "Dammapeta": "Khammam",
    "Mysuru": "Mysore",
    "Bengaluru Rural": "Bangalore Rural",
    "Bengaluru Urban": "Bangalore Urban",
    "Ahmednagar": "Ahilyanagar / Ahmednagar",
    #"Ahilyanagar": "Ahilyanagar / Ahmednagar",
    "Kanchipuram": "Kancheepuram",
    "Shimoga": "Shivamogga",
    "Mumbai City": "Mumbai"
}

total_updated = 0
for old_district, new_district in district_updates.items():
    stmt_district = (
        update(GeneralInformationSheet)
        .where(func.trim(GeneralInformationSheet.District) == old_district)
        .values(District=new_district)
    )
    result_district = session.execute(stmt_district)
    session.commit()
    print(f"✅ Replaced '{old_district}' with '{new_district}' in {result_district.rowcount} rows.")
    total_updated += result_district.rowcount

print(f"\n🎯 Total district records updated: {total_updated}")


stmt_states = (
    update(GeneralInformationSheet)
    .where(
        or_(
            func.trim(GeneralInformationSheet.District) == "Purba Medinipur",
            func.trim(GeneralInformationSheet.District) == "Agra",
            func.trim(GeneralInformationSheet.District) == "Thrissur",
            func.trim(GeneralInformationSheet.District) == "Kannur",
            func.trim(GeneralInformationSheet.District) == "Khagaria",
            func.trim(GeneralInformationSheet.District) == "Thiruvananthapuram",
            func.trim(GeneralInformationSheet.District) == "Bangalore Rural",
            func.trim(GeneralInformationSheet.District) == "New Delhi",
            func.trim(GeneralInformationSheet.District) == "Bangalore Urban"
        )
    )
    .values(
        State_Union_Territory=case(
            (func.trim(GeneralInformationSheet.District) == "Purba Medinipur", "West Bengal"),
            (func.trim(GeneralInformationSheet.District) == "Agra", "Uttar Pradesh"),
            (func.trim(GeneralInformationSheet.District) == "Thrissur", "Kerala"),
            (func.trim(GeneralInformationSheet.District) == "Kannur", "Kerala"),
            (func.trim(GeneralInformationSheet.District) == "Khagaria", "Bihar"),
            (func.trim(GeneralInformationSheet.District) == "Thiruvananthapuram", "Kerala"),
            (func.trim(GeneralInformationSheet.District) == "Bangalore Rural", "Karnataka"),
            (func.trim(GeneralInformationSheet.District) == "New Delhi", "Delhi"),
            (func.trim(GeneralInformationSheet.District) == "Bangalore Urban", "Karnataka"),
            else_=GeneralInformationSheet.State_Union_Territory
        )
    )
)

result_states = session.execute(stmt_states)
session.commit()
print(f"\n✅ Updated State_Union_Territory for {result_states.rowcount} rows based on District.")
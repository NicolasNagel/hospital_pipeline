from sqlalchemy import Column, String, Float, DateTime, Integer
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class EncountersModel(Base):
    """Modelo de Tabela no Banco de Dados para 'encounters.csv'."""

    __tablename__ = 'raw_encounters'

    id = Column(String, primary_key=True, unique=True, nullable=False)
    start = Column(DateTime, nullable=False)
    stop = Column(DateTime, nullable=False)
    patient = Column(String, nullable=False)
    organization = Column(String, nullable=False)
    payer = Column(String, nullable=False)
    encountersclass = Column(String, nullable=True)
    code = Column(String, nullable=False)
    description = Column(String, nullable=False)
    base_encounter_cost = Column(Float, nullable=False)
    total_claim_cost = Column(Float, nullable=False)
    payer_coverage = Column(Float, nullable=False)
    reasoncode = Column(String, nullable=True)
    reasondescription = Column(String, nullable=True)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<EncountersModel(id={self.id} | patient={self.patient})>'
    
class OrganizationsModel(Base):
    """Modelo de Tabela no Banco de Dados para 'organizations.csv'."""

    __tablename__ = 'raw_organizations'

    id = Column(String, primary_key=True, nullable=False, unique=True)
    name = Column(String, nullable=False)
    address = Column(String, nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    zip = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<OrganizationsModel(id={self.id} | name={self.name})>'
    
class PatientsModel(Base):
    """Modelo de Tabela no Banco de Dados para 'patients.csv'."""

    __tablename__ = 'raw_patients'

    id = Column(String, primary_key=True, unique=True, nullable=False)
    birthdate = Column(DateTime, nullable=False)
    deathdate = Column(DateTime, nullable=True)
    prefix = Column(String, nullable=True)
    first= Column(String, nullable=False)
    last = Column(String, nullable=False)
    suffix= Column(String, nullable=True)
    maiden = Column(String, nullable=False)
    marital = Column(String, nullable=False)
    race = Column(String, nullable=False)
    ethnicity = Column(String, nullable=True)
    gender = Column(String, name=False)
    birthplace = Column(String, nullable=True)
    address = Column(String, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    county = Column(String, nullable=True)
    zip = Column(String, nullable=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<PatientsModel(id={self.id} | first={self.first} | last={self.last})>'
    
class PayersModel(Base):
    """Modelo de Tabela no Banco de Dados para 'payers.csv'."""

    __tablename__ = 'raw_payers'

    id = Column(String, primary_key=True, unique=True, nullable=False)
    name = Column(String, nullable=False)
    address = Column(String, nullable=True)
    city = Column(String, nullable=True)
    state_headquartered = Column(String, nullable=True)
    zip = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<PayersModel(id={self.id} | name = {self.name})>'
    
class ProceduresModel(Base):
    """Modelo de Tabela no Banco de Dados para 'procedures.csv'."""

    __tablename__ = 'raw_procedures'

    id = Column(Integer, nullable=False, primary_key=True, autoincrement=True)
    start = Column(DateTime, nullable=False)
    stop = Column(DateTime, nullable=False)
    patient = Column(String, nullable=False) 
    encounter = Column(String, name=False)
    code = Column(String, nullable=False)
    description = Column(String, nullable=False)
    base_cost = Column(Float, nullable=False)
    reasoncode = Column(String, nullable=True)
    reasondescription = Column(String, nullable=True)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f'<ProceduresModel(start={self.start} | stop={self.stop} | patient={self.patient})>'
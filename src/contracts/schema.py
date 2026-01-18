import pandera as pa

from datetime import datetime, date
from pandera.typing import Series


class EncontersSchema(pa.DataFrameModel):
    """Schema de validação para o arquivo de 'encounter.csv'."""

    id: Series[str] = pa.Field(unique=False, nullable=False)
    start: Series[datetime] = pa.Field(nullable=False)
    stop: Series[datetime] = pa.Field(nullable=False)
    patient: Series[str] = pa.Field(nullable=False)
    organization: Series[str] = pa.Field(unique=False, nullable=False)
    payer: Series[str] = pa.Field(unique=False, nullable=False)
    encounterclass: Series[str] = pa.Field(isin=['ambulatory', 'outpatient', 'inpatient', 'wellness', 'urgentcare', 'emergency'], nullable=False)
    code: Series[str] = pa.Field(nullable=False)
    description: Series[str] = pa.Field(nullable=False)
    base_encounter_cost: Series[float] = pa.Field(ge=0)
    total_claim_cost: Series[float] = pa.Field(ge=0)
    payer_coverage: Series[float] = pa.Field(ge=0)
    reasoncode: Series[str] = pa.Field(nullable=True)
    reasondescription: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = True

class OrganizationsSchema(pa.DataFrameModel):
    """Schema de validação para o arquivo 'organizations.csv'."""

    id: Series[str] = pa.Field(unique=True, nullable=False)
    name: Series[str] = pa.Field(nullable=False)
    address: Series[str] = pa.Field(nullable=False)
    city: Series[str] = pa.Field(nullable=False)
    state: Series[str] = pa.Field(nullable=False)
    zip: Series[str] = pa.Field(nullable=False)
    lat: Series[float] = pa.Field(nullable=False)
    lon: Series[float] = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = True

class PatientsSchema(pa.DataFrameModel):
    """Schema de validação para o arquivo 'patients.csv'."""

    id: Series[str] = pa.Field(nullable=False)
    birthdate: Series[date] = pa.Field(nullable=False)
    deathdate: Series[date] = pa.Field(nullable=True)
    prefix: Series[str] = pa.Field(nullable=True)
    first: Series[str] = pa.Field(nullable=True)
    last: Series[str] = pa.Field(nullable=True)
    suffix: Series[str] = pa.Field(nullable=True)
    maiden: Series[str] = pa.Field(nullable=True)
    marital: Series[str] = pa.Field(isin=['M','S', 'D', 'W'], nullable=True)
    race: Series[str] = pa.Field(nullable=True)
    ethnicity: Series[str] = pa.Field(nullable=True)
    gender: Series[str] = pa.Field(isin=['M', 'F'], nullable=True)
    birthplace: Series[str] = pa.Field(nullable=True)
    address: Series[str] = pa.Field(nullable=False)
    city: Series[str] = pa.Field(nullable=False)
    state: Series[str] = pa.Field(nullable=False)
    county: Series[str] = pa.Field(nullable=False)
    zip: Series[str] = pa.Field(nullable=True)
    lat: Series[float] = pa.Field(nullable=False)
    lon: Series[float] = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = True

class PayersSchema(pa.DataFrameModel):
    """Schema de validação para o arquivo 'payers.csv'."""

    id: Series[str] = pa.Field(nullable=False)
    name: Series[str] = pa.Field(nullable=False)
    address: Series[str] = pa.Field(nullable=True)
    city: Series[str] = pa.Field(nullable=True)
    state_headquartered: Series[str] = pa.Field(nullable=True)
    zip: Series[str] = pa.Field(nullable=True)
    phone: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = True

class ProceduresSchema(pa.DataFrameModel):
    """Schema de validação para o arquivo 'procedures.csv'."""

    start: Series[datetime] = pa.Field(nullable=False)
    stop: Series[datetime] = pa.Field(nullable=False)
    patient: Series[str] = pa.Field(nullable=False)
    encounter: Series[str] = pa.Field(nullable=False)
    code: Series[str] = pa.Field(nullable=False)
    description: Series[str] = pa.Field(nullable=False)
    base_cost: Series[float] = pa.Field(nullable=True)
    reasoncode: Series[str] = pa.Field(nullable=True)
    reasondescription: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = True
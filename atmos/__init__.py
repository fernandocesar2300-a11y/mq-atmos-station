"""
DSS FIELD · MQ ATMOS ENGINE
Single meteorological engine for DreamSports events.

Products:
  status.json  nowcast per sector (+0 / +3 / +6 h) with storm, snow, heat, MRI
  field.json   7-day forecast per point and hour inside the riding window,
               with ensemble probabilities (ECMWF IFS 51 members)

Everything is configured per event in events/<name>.yaml.
Language of code and JSON: English. Public labels: English / Portuguese.
"""
ENGINE_VERSION  = "22.0"
SCHEMA_VERSION  = 3

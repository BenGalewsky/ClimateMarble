FROM python:3.6-buster
WORKDIR /climatemarble
RUN apt-get update && apt-get install -y gfortran
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY SAMPLE2GRID_SW.F .
RUN f2py -m sample2grid_sw -c SAMPLE2GRID_SW.F

COPY *.py ./


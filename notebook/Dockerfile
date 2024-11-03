FROM jupyter/pyspark-notebook:latest

WORKDIR /home/jovyan/work

COPY postgresql-42.5.1.jar ./
COPY data_analysis.ipynb ./

RUN pip install --upgrade pip
RUN pip install pandas numpy psycopg2-binary matplotlib sqlalchemy regex seaborn pyspark bar_chart_race

USER $NB_UID
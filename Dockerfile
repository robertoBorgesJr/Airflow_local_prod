# Dockerfile
FROM apache/airflow:2.5.1

USER root

# Instala pacotes adicionais
RUN pip install --no-cache-dir \
    pandas \
    openpyxl \
    xlrd \
    pyarrow \
    plotly 

USER airflow

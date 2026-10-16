FROM astrocrpublic.azurecr.io/runtime:3.3-9

# Permite importar o pacote src/ (Extract, Transformer, Loader) dentro das DAGs
ENV PYTHONPATH=/usr/local/airflow

FROM python:3.7-slim-buster

#RUN apt-get update && apt-get -yq install libmariadb-dev-compat libmariadb-dev
#WORKDIR /app
#COPY deploy/requirements.txt deploy/wheels.tar /app/
#RUN tar -xvf wheels.tar --one-top-level
#RUN pip3 install --no-index --find-links=./wheels -r requirements.txt

#SQL server driver telepítése (debian 10)
#https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16&tabs=debian18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline
RUN apt install -y curl gnupg unixodbc-dev
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
#ellenőrzés
#python3.7
#>>>import pyodbc
#>>>pyodbc.drivers()

# fejlesztőkörnyezetben nem átmásoljuk a fájlokat az image-be, hanem volume-ként csatoljuk
# a programkódot tartalmazó mappát a /app/ mappához a `docker run` parancs kiadásakor
COPY egis.py /app/
COPY app /app/app
# CMD-vel mondjuk meg a dockernek hogy a container futtatásakor ezt a parancsot futtassa
CMD ["python3.7", "/app/egis.py"]
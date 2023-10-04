# SQL server driver telepítése (Debian 10)

## Hivatalos dokumentáció  
https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16&tabs=debian18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline

## Telepítés
apt install -y curl gnupg unixodbc-dev  
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -  
curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/  mssql-release.list   
apt-get update  
ACCEPT_EULA=Y apt-get install -y msodbcsql18

## Ellenőrzés
python  
import pyodbc  
pyodbc.drivers()  
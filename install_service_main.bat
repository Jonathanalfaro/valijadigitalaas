@echo off
cd /d "C:\Users\Jonas\PycharmProjects\valijadigitalaas"
echo Instalando servicio Valija Digital...

REM Instalar pywin32
"C:\Users\Jonas\miniconda3\envs\valijadigitalaas\python.exe" -m pip install pywin32

REM Instalar el servicio
"C:\Users\Jonas\miniconda3\envs\valijadigitalaas\python.exe" service_valija_digital.py install

REM Configurar el servicio con el intérprete correcto
sc config ValijaDigital binPath= "\"C:\Users\Jonas\miniconda3\envs\valijadigitalaas\python.exe\" \"C:\Users\Jonas\PycharmProjects\valijadigitalaas\service_valija_digital.py\""
sc config ValijaDigital start= auto

echo Servicio instalado correctamente.
echo Para iniciar: net start ValijaDigital
echo Para detener: net stop ValijaDigital
echo Para desinstalar: python service_valija_digital.py remove
pause
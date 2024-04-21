import schedule
import time
import subprocess

def etl_emissions():
    subprocess.run(["python", "EmissionsEtl.py"])

def etl_evpopulation():
    subprocess.run(["python", "EVPopulation.py"])

def etl_openmapchargingstations():
    subprocess.run(["python", "OpenMapChargingStations.py"])


schedule.every().day.at("02:00").do(etl_emissions)
schedule.every().day.at("04:00").do(etl_evpopulation)
schedule.every().day.at("06:00").do(etl_openmapchargingstations)

#  Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(60)

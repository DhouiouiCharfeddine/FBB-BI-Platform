import os
import subprocess
import time
import win32serviceutil
import win32service
import win32event
import servicemanager
import schedule

class ETLService(win32serviceutil.ServiceFramework):
    _svc_name_ = "MyService"
    _svc_display_name_ = "My Service"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        schedule.every(15).minutes.do(self.run_ingest_data)
        schedule.every(20).minutes.do(self.run_etl)
        
        self.run_etl()  # Run ETL immediately
        
        while True:
            schedule.run_pending()
            time.sleep(1)

    def run_ingest_data(self):
        print("Running IngestData.py...")
        subprocess.run(["python", os.path.join("C:\\Users\\hp\\OneDrive\\Bureau\\End_Studies_Project", "Ingest_RouterData.py")])

    def run_etl(self):
        print("Running ETL.py...")
        subprocess.run(["python", os.path.join("C:\\Users\\hp\\OneDrive\\Bureau\\End_Studies_Project", "ETL_Routers.py")])

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(ETLService)

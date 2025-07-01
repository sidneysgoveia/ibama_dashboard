import threading
import time
from datetime import datetime, timedelta
from pytz import timezone
from apscheduler.schedulers.background import BackgroundScheduler
from decouple import config
import logging

class DataRefreshScheduler:
    def __init__(self, data_loader=None, database=None):
        """Initialize the data refresh scheduler"""
        self.data_loader = data_loader
        self.database = database
        self.scheduler = BackgroundScheduler()
        self.logger = self._setup_logger()
        self.refresh_complete = False
        self.last_refresh_time = None
        self.next_refresh_time = None
        self.error_message = None
        
    def _setup_logger(self):
        """Set up logging for the scheduler"""
        logger = logging.getLogger('data_refresh_scheduler')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
        
    def start(self):
        """Start the scheduler"""
        if not self.data_loader:
            self.logger.error("Data loader not initialized")
            return False
            
        if not self.database:
            self.logger.error("Database not initialized")
            return False
            
        # Define Brazil timezone
        brazil_tz = timezone('America/Sao_Paulo')
        
        # Schedule daily refresh at 10:00 AM Brasilia time
        self.scheduler.add_job(
            self._refresh_data,
            'cron',
            hour=10,
            minute=0,
            timezone=brazil_tz
        )
        
        # Calculate next run time
        now = datetime.now(brazil_tz)
        target_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
        
        if now > target_time:
            # If current time is past 10 AM, schedule for tomorrow
            target_time = target_time + timedelta(days=1)
        
        self.next_refresh_time = target_time
            
        # Start the scheduler
        try:
            self.scheduler.start()
            self.logger.info(f"Scheduler started. Next refresh at {target_time}")
            
            # Run initial data load in a separate thread
            threading.Thread(target=self._initial_data_load).start()
            
            return True
        except Exception as e:
            self.logger.error(f"Error starting scheduler: {e}")
            return False
            
    def _initial_data_load(self):
        """Run initial data load when app starts"""
        self.logger.info("Running initial data load")
        self._refresh_data()
        
    def _refresh_data(self):
        """Refresh data from source"""
        self.refresh_complete = False
        self.error_message = None
        
        try:
            self.logger.info("Starting data refresh")
            
            # Process and store data
            success = self.data_loader.process_and_store_data()
            
            if success:
                self.refresh_complete = True
                self.last_refresh_time = datetime.now(timezone('America/Sao_Paulo'))
                
                # Calculate next refresh time
                now = datetime.now(timezone('America/Sao_Paulo'))
                target_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
                
                if now > target_time:
                    # If current time is past 10 AM, schedule for tomorrow
                    target_time = target_time + timedelta(days=1)
                
                self.next_refresh_time = target_time
                
                self.logger.info(f"Data refresh completed successfully. Next refresh at {self.next_refresh_time}")
            else:
                self.error_message = "Data refresh failed"
                self.logger.error(self.error_message)
        except Exception as e:
            self.error_message = str(e)
            self.logger.error(f"Error refreshing data: {e}")
            
    def get_status(self):
        """Get scheduler status"""
        return {
            "last_refresh": self.last_refresh_time.strftime("%Y-%m-%d %H:%M:%S") if self.last_refresh_time else None,
            "next_refresh": self.next_refresh_time.strftime("%Y-%m-%d %H:%M:%S") if self.next_refresh_time else None,
            "refresh_complete": self.refresh_complete,
            "error": self.error_message
        }
        
    def manual_refresh(self):
        """Manually trigger a data refresh"""
        threading.Thread(target=self._refresh_data).start()
        return True
        
    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            self.logger.info("Scheduler stopped")
            return True
        return False 
from datetime import datetime, timedelta

now = datetime.utcnow()
start = now - timedelta(minutes=2)   # ensure window already opened
end   = now + timedelta(days=1)      # or use start for one-shot

body["transferJob"]["schedule"] = {
    "scheduleStartDate": {"year": start.year, "month": start.month, "day": start.day},
    "startTimeOfDay":    {"hours": start.hour, "minutes": start.minute},
    "scheduleEndDate":   {"year": end.year,   "month": end.month,   "day": end.day},
}

import datetime
today = datetime.date.today()
nextday = today + datetime.timedelta(days=1)
next2day = today + datetime.timedelta(days=2)
previousday = today - datetime.timedelta(days=1)
previous2day = today - datetime.timedelta(days=2)

print(previous2day)
print(previousday)
print(today)
print(nextday)
print(next2day)


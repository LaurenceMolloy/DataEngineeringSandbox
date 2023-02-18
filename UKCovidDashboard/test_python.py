from datetime import datetime
time = datetime.utcnow().strftime("%H:%M:%S")
with open('out.txt', 'w') as file:
    file.write(time)
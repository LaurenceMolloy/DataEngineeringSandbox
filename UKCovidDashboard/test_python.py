from datetime import datetime
import os

#script_path = os.path.realpath(os.path.dirname(__file__))
parent_path = os.path.normpath(os.path.join('.', os.pardir))
file_path = os.path.join(parent_path,'docs', 'images', 'out.txt')
print(file_path)
time = datetime.utcnow().strftime("%H:%M:%S")
with open("out.txt", 'w') as file:
    file.write(time)
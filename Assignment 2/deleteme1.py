import requests
import yaml
import schedule
# pipenv run python client.py
import time

def yaml_loader(filepath):
# load yaml file"
    with open(filepath, 'r') as file_descriptor:
        data = yaml.load(file_descriptor)
    return data

def yaml_dump(filepath):
# dumps data to a yaml file"
    with open(filepath, 'w') as file_descriptor:
        yaml.dump(data,file_descriptor)
# filepath2 = "test2,yaml"
# data2 = {
#     "items":{
#         "sword":100,
#         "axe":80,
#         "boots":40
#     }
# }
# yaml_dump(filepath2,data2)

if __name__=="__main__":
    filepath ="input.yaml"
    data = yaml_loader(filepath)
    print (data)
    # {'Step': {'outbound_url': 'http://requestbin.com/', 'id': 1, 'condition': {'then': {'action': 'print', 'data': 'http.response.body'}, 'if': {'equal': {'right': 200, 'left': 'http.response.code'}}, 'else': {'action': 'print', 'data': 'Error'}}, 'method': 'GET'}, 'Scheduler': {'when': '5 * * * *', 'step_id_to_execute': [1]}}

    step = data.get('Step')
    for step_name, step_value in step.items():
        print (step_name, step_value)
    # id 1
    # condition {'then': {'action': 'print', 'data': 'http.response.body'}, 'if': {'equal': {'right': 200, 'left': 'http.response.code'}}, 'else': {'action': 'print', 'data': 'Error'}}
    # method GET

    scheduler = data.get('Scheduler')
    for scheduler_name, scheduler_value in scheduler.items():
        print (scheduler_name, scheduler_value)
       
    # ('when', '5 * * * *')
    # ('step_id_to_execute', [1])


# # minute
# schedule.every(5).minutes.do(job) 
# # hour
# schedule.every().hour.do(job)
# # day of month
# # day of week
# schedule.every().day.at("10:30").do(job)
# schedule.every(5).to(10).minutes.do(job)
# schedule.every().monday.do(job)
# schedule.every().wednesday.at("13:15").do(job)
# schedule.every().minute.at(":17").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

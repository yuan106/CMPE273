import requests
import yaml
import schedule
import sys
import time


# creating an empty dictionary
flow = {}

def yaml_loader(filepath):
    # load yaml file"
    with open(filepath, 'r') as file_descriptor:
        data = yaml.load(file_descriptor, yaml.FullLoader)
    return data


def http_client(method, outbound_url):
    #make a http call
    response = requests.request(method, outbound_url)
    return response

def parseData(key, httpResponse):
    if isinstance(key, str) and key.startswith('http.response'):
        splitted_key = key.split('.')
        if(splitted_key[2] == 'code'):
            return httpResponse.status_code
        elif(splitted_key[2] == 'body'):
            return httpResponse.text
        elif(splitted_key[2] == 'headers'):
            header_key = splitted_key[3].lower()
            for k, v in httpResponse.headers.items():
                if k.lower() == header_key:
                    return v
            
    else:
        return key

def handleTypeHttpClient(step_data, input_data=None):

    if not "method" in step_data or not step_data["method"]:
        print("invalid method")
        return

    if not "outbound_url" in step_data or not step_data["outbound_url"]:
        print("invalid outbound_url")
        return

    method = step_data["method"]

    outbound_url = step_data["outbound_url"]

    if outbound_url == "::input:data":
        outbound_url = input_data

    response = http_client(method, outbound_url)
    # print(response) 
    # <Response [200]>

    # condition = condition(response)
    if "condition" in step_data:
        condition = step_data["condition"]
        # print(condition) 

        # Validate condition object
        if not "if" in condition:
            print("'if' key missing from condition")
            return
        
        if not "then" in condition:
            print("'then' key missing from condition")
            return

        ifs = condition["if"]
        then = condition["then"]

        # Validate then object
        if not "action" in then:
            print("'action' key missing from then")
            return
        
        if not "data" in then:
            print("'data' key missing from then")
            return

        # print(ifs)
        if "equal" in ifs:
            equal = ifs["equal"]

            if not "left" in equal:
                print("'left' key missing from condition")
                return
            
            if not "right" in equal:
                print("'right' key missing from condition")
                return

            left = parseData(equal["left"], response)
            right = parseData(equal["right"], response)

            # if left == right:
            if left == right:

                then_action = then["action"]
                then_data = parseData(then["data"], response)

                if then_action == "::print":
                    print(then_data)
                elif then_action.startswith("::invoke:step:"):
                    next_step_id = int(then_action.split(":")[4])
                    run_step(next_step_id, then_data)
                else:
                    print("Invalid action in 'then'")

            
            else:
                if "else" in condition:
                    _else = condition["else"]

                    # Validate else object
                    if not "action" in _else:
                        print("'action' key missing from else")
                        return
                    
                    if not "data" in _else:
                        print("'data' key missing from else")
                        return

                    else_action = _else["action"]
                    else_data = parseData(_else ["data"], response)
                    
                    if else_action == "::print":
                        print(else_data)
                    elif else_action.startswith("::invoke:step:"):
                        next_step_id = int(else_action.split(":")[4])
                        run_step(next_step_id, else_data)
                    else:
                        print("Invalid action in 'else'")

def run_step(id, input_data=None):

    # Retrieve step data
    step_data = {}

    if "Steps" not in flow:
        print('Flow does not have steps:', flow)
        return

    for step in flow["Steps"]:
        if id in step.keys():
            step_data = step[id]
            break

    if not step_data:
        print("Step ID does not exist.")
        return

    # Execute step based on type
    if "type" in step_data and step_data["type"]:
        
        _type = step_data["type"]

        if _type == "HTTP_CLIENT": 
            handleTypeHttpClient(step_data, input_data)
        else:
            print("Invalid type")
   
def job():
    for i in flow["Scheduler"]["step_id_to_execute"]:
        run_step(i)


def monday(time4):
    return schedule.every().monday.at(time4).do(job)

def tuesday(time4):
    return schedule.every().tuesday.at(time4).do(job)
                
def wednesday(time4):
    return schedule.every().wednesday.at(time4).do(job)
                 
def thursday(time4):
    return schedule.every().thursday.at(time4).do(job)
                
def friday(time4):
    return schedule.every().friday.at(time4).do(job)
                  
def saturday(time4):
    return schedule.every().saturday.at(time4).do(job)
                 
def sunday(time4):
    return schedule.every().sunday.at(time4).do(job)

def run():
    if "Scheduler" in flow:
        if "when" in flow["Scheduler"] and "step_id_to_execute" in flow["Scheduler"]:
            # return flow["Scheduler"]
            splited_when = flow["Scheduler"]["when"].split()
            # return splited_when  
            # ['5', '*', '*']
            # return splited_when[0]
            # 5

            # 5 * * 
            if splited_when[1]=="*" and splited_when[2]=="*": 
                min = int(splited_when[0])
                case1 = schedule.every(min).minutes.do(job) 
                return case1

            # * 2 * 
            #  5 1 * 
            elif splited_when[2]=="*":
                hour = int(splited_when[1])
                if splited_when[0] =="*":
                    min2 = 0
                    time2 ="{0:0=2d}:{1:0=2d}".format(hour,min2)
                    case2 = schedule.every().day.at(time2).do(job)
                    return case2
                else:
                    min3 = int(splited_when[0])
                    time3 ="{0:0=2d}:{1:0=2d}".format(hour,min3)
                    case3 = schedule.every().day.at(time3).do(job)
                    return case3
            else:
                if splited_when[0] =="*" and splited_when[1] =="*":
                    hour4 = 0
                    min4 = 0
                    time4 ="{0:0=2d}:{1:0=2d}".format(hour4,min4)
                else: 
                    hour5 = int(splited_when[1])
                    min5 = int(splited_when[0])
                    time4 ="{0:0=2d}:{1:0=2d}".format(hour5,min5)
                # print (time4)
                day = int (splited_when[2])
                if day == 1:
                    case4 = monday(time4)
                    return case4
                elif day == 2:
                    case4 = tuesday(time4)
                    return case4
                elif day == 3:
                    case4 = wednesday(time4)
                    return case4
                elif day == 4:
                    case4 = thursday(time4)
                    return case4
                elif day == 5:
                    case4 = friday(time4)
                    return case4
                elif day == 6:
                    case4 = saturday(time4)
                    return case4
                elif day == 0:
                    case4 = sunday(time4)
                    return case4
        
               
        # 0 1 2
        # 5 * * # Executes at every 5 minutes =
        #   0 1 2
        ##  * 2 * # Executes at every day at 02:00 =
        ##  5 1 * # Executes at 1:05 =
        ## 10 23 * # Executes at 23:10

        # * * 1 # Executes at every Monday at 00:00
        # 10 23 1 # Executes at 23:10 on every Monday
        # schedule.every().wednesday.at("13:15").do(job)


def main():

    global flow

    filepath = sys.argv[1]
    flow = yaml_loader(filepath)
    # print (flow)
    # {'Steps': [{1: {'type': 'HTTP_CLIENT', 'method': 'GET', 'outbound_url': 'http://requestbin.com/', 'condition': {'if': {'equal': {'left': 'http.response.code', 'right': 200}}, 'then': {'action': '::print', 'data': 'http.response.body'}, 'else': {'action': '::print', 'data': 'Error'}}}}], 'Scheduler': {'when': '5 * *', 'step_id_to_execute': [1]}}
    
    results = run()
    print(results)
    while True:
        schedule.run_pending()
        time.sleep(1)

    # run_step(1)

if __name__=="__main__":
    main()
    



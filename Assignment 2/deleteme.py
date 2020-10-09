import schedule
import time

def http_client(config):
    #make a http call
    return "response"

#condition is if then else
def eval_condition(data, condition):
    #evaluate the condition against the data
    pass

def run_step():
    print("execution the step")
    if type is 'type: HTTP_CLIENT':
        config = {
            "http_verb" : 'GET',
            "url" : "https://enq4hm6lhvb7l.x.pipedream.net"
        
        }
        response = http_client(config)
        condition = { "fix me "}
        eval_condition(response, condition)
    else:
        print("invalid type")

def set_scheduler():
    mm = 5
    schedule.every(mm).minutes.do(run_step)

set_sheduler()

while True:
    schedule.run_pending()
    time.sleep(1)
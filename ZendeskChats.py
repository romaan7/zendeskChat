#API Documentation: https://developer.zendesk.com/rest_api/docs/chat/chats

import requests 
import json
import pandas as pd
import datetime
import time
import logging
from flatten_json import flatten 

#Create logger
logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")

# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


#Authorization Data
USER = ""
PASSWORD = ""

#Rate Limit the API calls
MAX_CALLS_PER_MIN = 100
TIME_PERIOD  = 60
CALL_COUNTER = 0

#API URLs for 
BASE_PATH = "https://www.zopim.com/api/v2"
CHAT_PATH = "/chats"
CHAT_SEARCH_PATH = "/chats/search"

#API configurationb
config = {'username':USER,
    'password':PASSWORD,
    'base_path': BASE_PATH,
    'chat_path': CHAT_PATH,
    'chat_search_path': CHAT_SEARCH_PATH
    }

#serializing the search into smaller chunks by limiting results by timestamp range. 
#https://developer.zendesk.com/rest_api/docs/chat/chats#rate-limit
start_date = "2020-03-01"
end_date = "2020-03-30"


#CSV output file name
OUTPUT_FILE = "ZendeskChats_{0}-TO-{1}-ON-{2}.csv".format(start_date,end_date,datetime.datetime.now().strftime("%d%m%y"))
logger.info("Output will be written to file: {0}".format(OUTPUT_FILE))

def get_chat_ids_for_date_range(startDate, endDate, xsession):
    global CALL_COUNTER, TIME_PERIOD
    all_chat_ids = []
    search_query = "timestamp:[{0} TO {1}]".format(startDate,endDate)
    params = {"q":search_query}
    url = "{0}{1}".format(config['base_path'],config['chat_search_path'])
    logger.info("Retriving Chats between period: {0} to {1}".format(startDate,endDate))
    while url:
        if (CALL_COUNTER == MAX_CALLS_PER_MIN):
            logger.warning("Rate limit reached. Sleeping for {0} seconds".format(TIME_PERIOD))
            time.sleep(TIME_PERIOD)
            logger.info("Resuming..Total chats featched so far: {0}".format(len(all_chat_ids)))
            CALL_COUNTER = 0
        r = xsession.get(url, params=params)
        CALL_COUNTER += 1
        if r.status_code == 200:
            response = json.loads(r.text)
            for chat in response['results']:
                all_chat_ids.append(chat['id'])
                url = response['next_url']
        else:
            logger.error("A request failed in the loop")
            logger.error(str("Tried URL:{0}").format(url))
            logger.error(str("Got a response code:{0}").format(r.status_code))
            exit(1)
    logger.info("Total chats Retrived: {0}".format(len(all_chat_ids)))
    return all_chat_ids

def get_chat_data(chatID, xsession):
    global CALL_COUNTER, TIME_PERIOD
    chat_data = []
    #logger.info(str("Request No:{0} | Getting Data for: {1}").format(CALL_COUNTER,chatID))
    url = "{0}{1}/{2}".format(config['base_path'],config['chat_path'],chatID)
    if (CALL_COUNTER == MAX_CALLS_PER_MIN):
        logger.warning(str("Rate limit reached. Sleeping for {0} seconds").format(TIME_PERIOD))
        time.sleep(TIME_PERIOD)
        logger.info("Resuming..")
        CALL_COUNTER = 0
    r = xsession.get(url)
    CALL_COUNTER += 1
    if r.status_code == 200:
            response = json.loads(r.text)
            chat_data.append(response)
    else:
       logger.error("A error occured while getting details for chat with chatid: {0}".format(chatID))
       logger.error(str("Tried URL:{0}").format(url))
       logger.error(str("Got a response code:{0}").format(r.status_code))
       exit(1)
    return flatten(response)

def write_to_csv(json_input,type):
    #df = pd.DataFrame.from_dict(json_input, orient="index")
    #df.T.to_csv(OUTPUT_FILE, mode='a', header = True, index = False)
    df = pd.DataFrame(json_input)
    df.to_csv(type+"_"+OUTPUT_FILE, mode='a', header = True, index = False)
    return True

#Create new session with zopim.com
session = requests.Session()
session.auth = (config['username'],config['password'])
session.headers.update({"content-type":"application/json"}) 

chatids = get_chat_ids_for_date_range(start_date,end_date,session)

#Two types of chats are returned
support_chats = []
offline_messages = []

for chatid in chatids:
    data = get_chat_data(chatid,session)
    if data["type"] == "offline_msg":
        offline_messages.append(data)
    elif data["type"] == "chat":
        support_chats.append(data)
        
#Print status every 100 records, write to temp file every 500 records
    if len(support_chats) % 100 == 0 and len(support_chats) > 0:
        logger.info("Completed {} of {} chats".format(len(support_chats), len(chatids)))    
    if len(support_chats) % 500 == 0:
        write_to_csv(support_chats,"tmp_supportChats")

#Print status every 100 records, write to temp file every 500 records
    if len(offline_messages) % 100 == 0 and len(offline_messages) > 0:
        logger.info("Completed {} of {} chats".format(len(offline_messages), len(chatids)))
    if len(offline_messages) % 500 == 0:
        write_to_csv(offline_messages,"tmp_offlineMessages")
    
#Finally writing to file
write_to_csv(support_chats,"supportChats")
write_to_csv(offline_messages,"offlineMessages")


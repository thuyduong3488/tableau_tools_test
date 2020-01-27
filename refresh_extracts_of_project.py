from config import CONFIG
from tableau_rest_api import *
from util.logger import Logger
import os,base64,time

proj_name_refresh = CONFIG['proj_name_refresh'][0]
server = CONFIG['server'][0]
log_path = CONFIG['log_path'][0]
# Concatenate log file
log_file = str(log_path) + '\\refresh_extracts_' + str(proj_name_refresh) + '_' + time.strftime("%Y%m%d-%H%M%S") + '.log'

# Add log file for debug
logger = Logger(log_file)
logger.enable_debug_level()
logger.log("Refresh extract for Project: " + str(proj_name_refresh))

# Getting username and password from environment variables
username = CONFIG['user'][0]
password = CONFIG['password'][0]

# Sign in
default = TableauRestApiConnection(server=str(server), username=username, password=password, site_content_url=u'default')
default.signin()
default.enable_logging(logger)

try:
    workbooks_with_name = default.query_workbooks(project_name_or_luid=proj_name_refresh)
    for i in range(0,len(workbooks_with_name)):
        wb_luid = workbooks_with_name[i].get(u'id')
        wb_name = workbooks_with_name[i].get(u'name')
        response = default.update_workbook_now(wb_name_or_luid=wb_luid)
        logger.log("Extract refresh for workbook: " + str(wb_name))
        time.sleep(30)
except (AttributeError, TypeError) as e:
    print("Error occurred: ", e)

# Sign out
default.signout()


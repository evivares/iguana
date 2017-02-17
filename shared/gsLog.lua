require 'gsLogEvent'

gsLog = {startEpochMillis=nil, startTime=nil, logPrefix=nil}

gsLog.host = 'localhost' -- Splunk listener hostname
gsLog.port = 514 -- Splunk listener port

--[[ Default log message parameters ]]--
gsLog.message = {}
--gsLog.message.hostname = nil -- Init
gsLog.message.call_type = nil -- Log
gsLog.message.time_taken = nil -- Log
gsLog.message.message_id = nil -- Init
gsLog.message.log_level = nil -- Log
gsLog.message.application = nil -- Log
gsLog.message.script = nil -- Log
gsLog.message.method_name = nil -- Log
gsLog.message.timestamp = nil -- Log
gsLog.message.environment = nil -- Init
gsLog.message.hl7_messagetype = nil -- MessageDetail
gsLog.message.hl7_eventtype = nil -- MessageDetail
gsLog.message.client_id = nil -- MessageDetail
gsLog.message.patient_id = nil -- MessageDetail
gsLog.message.event_id = nil -- Log
--[[ END Default log message parameters ]]--

-- Static log levels
gsLog.level = {['Debug']='debug', ['Info']='info', ['Warning']='warn', ['Error']='error'}
-- Static Environment Types
gsLog.environment = {prod='p', uat='u', qa='q', dev='d'}

function gsLog.log(T)
   -- Validate gsLog initialization
   gsLog.isInitialized()
   -- Validate T.logLevel exists in gsLog.level table
   if not gsLog.isValidTableEntry(gsLog.level, T.logLevel) then
      error('Invalid logLevel! Must use gsLog.level entry.', 2)
   end
   -- Validate T.eventID exists in gsLogEvent table
   if not gsLog.isValidTableEntry(gsLogEvent, T.eventID) then
      error('Invalid eventID! Must use gsLogEvent entry.', 2)
   end
   
   gsLog.message.timestamp = getSQLTimeMillis()
   gsLog.message.time_taken = timeTakenMillis(gsLog.startEpochMillis)
   --gsLog.message.call_type = ''
   gsLog.message.log_level = T.logLevel
   gsLog.message.application = iguana.channelName()
   gsLog.message.script = gsLog.getScriptName()
   -- Identify Calling function
   gsLog.message.method_name = gsLog.getCallingFunction(debug.traceback())
   -- Capturing Event ID for reference to wiki page
   gsLog.message.event_id = T.eventID
   
   local apiResp = gsLog.processSyslogMessage()
   --local resp = gsLog.processLogMessage()
   return apiResp
end

function gsLog.processSyslogMessage()
   if not iguana.isTest() then
      if gsLog.socket then
         gsLog.socket:send(getSyslogMessage(gsLog.message)..'\r\n')
      else
         iguana.logWarning("No socket available to send data on...")
      end
   end
   
   return msg
end

function getSyslogMessage(T)
   --[[ https://tools.ietf.org/html/rfc5424 
   SAMPLE (omit carriage return):
   <165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - 
   ID47 [iut="3" eventSource="Application" eventID="1011"]
   ]]--
   
   -- BEGIN HEADER
   local _msg = '<'..'163'..'>' -- PRI
   _msg = _msg..'1'..' ' -- Version
   _msg = _msg..getSyslogTimeMillis()..' ' -- Timestamp
   _msg = _msg..iguana.webInfo().host..' ' -- Hostname
   _msg = _msg..'Iguana'..' ' -- Application
   _msg = _msg..'-'..' ' -- ProcID
   _msg = _msg..'GSLOG'..' ' -- MsgID
   -- END HEADER
   
   -- BEGIN Structured Data
   _msg = _msg..'['
   
   for k,v in pairs(T) do
      if v and v ~= '' then
         -- escape syslog specific specials
         v = tostring(v):gsub("\\", '\\\\')
         v = v:gsub('\"', '\\\"')
         v = v:gsub(']', '\\]')
         _msg = _msg..k..'='..'\"'..v..'\" '
      end
   end
   _msg = _msg:trimRWS()..']'
   -- END Structured Data
   
	return _msg
end

function gsLog.processLogMessage()
   -- \r\n added to ssist with Splunk log ingestion
   local msg = '\r\n'
   
   if gsLog.logPrefix ~= nil then
      msg = msg..'keyword="'..gsLog.logPrefix..'" '
   end
   
   for k,v in pairs(gsLog.message) do
      if v ~= nil then
         msg = msg..k..'="'..v..'" '
      end
   end
   -- Assist with Splunk log ingestion
   msg = msg..'\r\n'
   
   local messageId = nil
   if iguana.messageId ~= nil then
      messageId = iguana.messageId()
      if gsLog.message.log_level == 'debug' then
         iguana.logDebug(msg, messageId)
      elseif gsLog.message.log_level == 'info' then
         iguana.logInfo(msg, messageId)
      elseif gsLog.message.log_level == 'warn' then
         iguana.logWarning(msg, messageId)
      elseif gsLog.message.log_level == 'error' then
         iguana.logError(msg, messageId)
      end
	else
      if gsLog.message.log_level == 'debug' then
         iguana.logDebug(msg)
      elseif gsLog.message.log_level == 'info' then
         iguana.logInfo(msg)
      elseif gsLog.message.log_level == 'warn' then
         iguana.logWarning(msg)
      elseif gsLog.message.log_level == 'error' then
         iguana.logError(msg)
      end
   end
   
   return msg
end

function gsLog.messageDetail(T)
   -- Validate gsLog initialization
   gsLog.isInitialized()
   gsLog.message.hl7_messagetype = T.messageType
   gsLog.message.hl7_eventtype = T.eventType
   gsLog.message.client_id = T.client_id
   gsLog.message.patient_id = T.patient_id
end

function gsLog.Init(T)
   -- Validate T.environment exists in gsLog.level table
   if not gsLog.isValidTableEntry(gsLog.environment, T.environment) then
      error('Invalid environment! Must use gsLog.environment entry.', 2)
   end
   
   if T.logPrefix ~= nil then gsLog.logPrefix = T.logPrefix end
   gsLog.startEpochMillis = getEpochMillis()
   gsLog.startTime = getSQLTimeMillis()
   --gsLog.message.hostname = iguana.webInfo().host
   
   if iguana.messageId ~= nil then gsLog.message.message_id = iguana.messageId() end
   gsLog.message.environment = T.environment
   
   if not iguana.isTest() then
      local success, s = pcall(net.tcp.connect, {host=gsLog.host, port=gsLog.port, timeout=120})
      if not success then
         iguana.logWarning("Unable to open socket to Splunk listener")
      else
         gsLog.socket = s
      end
   end
end

function gsLog.getScriptName()
   local name = iguana.project.guid()
   name = name:sub(1,name:find('[^-]*$')-2)
   name = name:sub(name:find('[^-]*$'))
   
   if name == 'To' or name == 'From' then name = name..'Trans' end
   
   return name
end

function timeTakenMillis(startTimeEpockMillis)
   -- Return current Epoch milliseconds minus start Epoch milliseconds
   return getEpochMillis() - startTimeEpockMillis
end

function getSQLTimeMillis()
   -- Get program execution time
   local c = os.clock()
   -- Extract decimal millis   
   c = c - math.modf(c)
   -- Convert 0 milliseconds to string 000
   if c==0 then 
      c='000' 
   else 
      c=tostring(c):sub(3,5) 
   end
   -- Formate date/time to SQL standard format and append millis
   local tf=os.ts.date('!%Y-%m-%d %H:%M:%S.')..c
   return tf
end

function getSyslogTimeMillis()
   -- Get program execution time
   local c = os.clock()
   -- Extract decimal millis   
   c = c - math.modf(c)
   -- Convert 0 milliseconds to string 000
   if c==0 then 
      c='000' 
   else 
      c=tostring(c):sub(3,5) 
   end
   -- Formate date/time to SQL standard format and append millis
   local tf=os.ts.date('!%Y-%m-%dT%H:%M:%S.')..c..'Z'
   return tf
end

function getEpochMillis()
   -- Get program execution time
   local c = os.clock()
   -- Extract decimal millis   
   c = c - math.modf(c)
   -- Get Unix Epoch format time and add decimal
   local epochDecimal = os.ts.gmtime() + c
   -- Multiply by 1000 to convert to milliseconds
   local epochMillis = epochDecimal * 1000
   -- Extracted integer value to ensure only integer is kept
   local epochMillis = math.modf(epochMillis)
   return epochMillis
end

function gsLog.getCallingFunction(b)
   b = b:split('\n')
   for i=1, #b do
      if i > 1 and b[i]:find('gsLog.lua') then
         return b[i+1]:gsub('\t',''):gsub('"',"'")
      end
   end
end

function gsLog.isValidTableEntry(table, value)
   local r = false
   for k,v in pairs(table) do
      if v == value then r = true end
   end
   return r
end

function gsLog.isInitialized()
   local r = true
   if gsLog.startEpochMillis == nil then r = false end
   if gsLog.startTime == nil then r = false end
   --if gsLog.message.hostname == nil then r = false end
   if gsLog.message.environment == nil then r = false end
   if not r then error('gsLog has not been initialized! Call gsLog.Init() in main!', 3) end
   return r
end

local HELP_DEF_INIT=[[{
   "Desc": "Generates standardized Iguana log entries to be ingested by Splunk.
   <p>gsLog is comprised of 2 shared libraries: gsLog.lua and gsLogEvent.lua
   <p><b>gsLog.lua</b> - All related functions/methods are located within this library and can be call by using the 'gsLog.' namespace
   <p><b>gsLogEvents.lua</b> - All event ID's are available in this library within the 'gsLogEvent' namespace",
   "Returns": [
      {
         "Desc": "nothing."
      }
   ],
   "SummaryLine": "Generates standardized Iguana log entries to be ingested by Splunk.",
   "SeeAlso": [
      {
         "Title": "Splunk - Iguana Log Integration",
         "Link": "https://guardiansolutions.atlassian.net/wiki/display/PA/Splunk+-+Iguana+Log+Integration"
      },
      {
         "Title": "Log Management Strategy",
         "Link": "https://guardiansolutions.atlassian.net/wiki/display/PA/Log+Management+Strategy"
      }
   ],
   "Title": "gsLog.Init",
   "Usage": "gsLog.Init{environment=&#60;value&#62;, logPrefix=&#60;value&#62;}",
   "Parameters": [
      {
         "environment": {
            "Desc": "Environment type [dev,qa,uat,prod] <u>gsLog.environment</u>. "
         }
      },
      {
         "logPrefix": {
            "Desc": "(Optional) Keyword to allow Splunk ingestion of only intended messages <u>string</u>. "
         }
      }
   ],
   "Examples": [
      "<pre>gsLog.Init{environment=gsLog.environment.dev, logPrefix='Splunk'}</pre>"
   ],
   "ParameterTable": true
}]]

help.set{input_function=gsLog.Init, help_data=json.parse{data=HELP_DEF_INIT}}

local HELP_DEF_MSGDETAIL=[[{
   "Desc": "Specify message details for standardized logging to Splunk. All parameters are optional.
   <p>gsLog is comprised of 2 shared libraries: gsLog.lua and gsLogEvent.lua
   <p><b>gsLog.lua</b> - All related functions/methods are located within this library and can be call by using the 'gsLog.' namespace
   <p><b>gsLogEvents.lua</b> - All event ID's are available in this library within the 'gsLogEvent' namespace",
   "Returns": [
      {
         "Desc": "nothing."
      }
   ],
   "SummaryLine": "Specify message details for standardized logging to Splunk",
   "SeeAlso": [
      {
         "Title": "Splunk - Iguana Log Integration",
         "Link": "https://guardiansolutions.atlassian.net/wiki/display/PA/Splunk+-+Iguana+Log+Integration"
      },
      {
         "Title": "Log Management Strategy",
         "Link": "https://guardiansolutions.atlassian.net/wiki/display/PA/Log+Management+Strategy"
      }
   ],
   "Title": "gsLog.messageDetail",
   "Usage": "gsLog.messageDetail{messageType=&#60;value&#62;, eventType=&#60;value&#62;, client_id=&#60;value&#62;, patient_id=&#60;value&#62;}",
   "Parameters": [
      {
         "messageType": {
            "Desc": "(Optional) Message type code <u>string</u>. "
         }
      },
      {
         "eventType": {
            "Desc": "(Optional) Event type code <u>string</u>. "
         }
      },
      {
         "client_id": {
            "Desc": "(Optional) Client ID from message <u>string</u>. "
         }
      },
      {
         "patient_id": {
            "Desc": "(Optional) Patient ID from message (Be cautious of PHI and HIPAA compliance) <u>string</u>. "
         }
      }
   ],
   "Examples": [
      "<pre>gsLog.messageDetail{messageType='ADT', eventType='A01', client_id='QAT2987987A31', patient_id='(A934J552134A'}</pre>"
   ],
   "ParameterTable": true
}]]

help.set{input_function=gsLog.messageDetail, help_data=json.parse{data=HELP_DEF_MSGDETAIL}}

local HELP_DEF_LOG=[[{
   "Desc": "Initiate standardized logging message for Splunk ingestion.
   <p>gsLog is comprised of 2 shared libraries: gsLog.lua and gsLogEvent.lua
   <p><b>gsLog.lua</b> - All related functions/methods are located within this library and can be call by using the 'gsLog.' namespace
   <p><b>gsLogEvents.lua</b> - All event ID's are available in this library within the 'gsLogEvent' namespace",
   "Returns": [
      {
         "Desc": "The logged message <u>string</u>."
      }
   ],
   "SummaryLine": "Initiate standardized logging message for Splunk ingestion.",
   "SeeAlso": [
      {
         "Title": "Splunk - Iguana Log Integration",
         "Link": "https://guardiansolutions.atlassian.net/wiki/display/PA/Splunk+-+Iguana+Log+Integration"
      },
      {
         "Title": "Log Management Strategy",
         "Link": "https://guardiansolutions.atlassian.net/wiki/display/PA/Log+Management+Strategy"
      }
   ],
   "Title": "gsLog.log",
   "Usage": "gsLog.log{logLevel=&#60;value&#62;, eventID=&#60;value&#62;}",
   "Parameters": [
      {
         "logLevel": {
            "Desc": "Logging level [Debug, Info, Warning, Error] <u>gsLog.Level</u>. "
         }
      },
      {
         "eventID": {
            "Desc": "Logging Event Identifier <u>gsLogEvent</u>. "
         }
      }
   ],
   "Examples": [
      "<pre>gsLog.log{logLevel=gsLog.Level.Info, eventID=gsLogEvent.AnnounceMessageProcessing}</pre>"
   ],
   "ParameterTable": true
}]]

help.set{input_function=gsLog.log, help_data=json.parse{data=HELP_DEF_LOG}}

return gsLog


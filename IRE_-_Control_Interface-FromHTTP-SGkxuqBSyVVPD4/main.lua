require 'Webpage'  -- Contains our default webpage html string.
require 'gsmos'
require 'gsLog'
require 'IRE_RuleStore'

function main(Data)
   -- Initialize gsLog
   gsLog.Init{environment='p',logPrefix='Splunk'}
   gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.AnnounceMessageProcessing}
   --iguana.logInfo('PostData:\n\n'..Data)
   trace(Data)
   iguana.stopOnError(false)
   -- Parse each incoming request with net.http.parseRequest
   local Request = net.http.parseRequest{data=Data}
   local bearer = Request.headers.bearer
   local body = Request.body
   
   -- Authentication
   if bearer ~= gsmos.token then
      net.http.respond{body="Authentication Error",code=205}
      gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusAuthenticationError}
      return
   end
   
   local v, res, code = validateRequest(Request.method, Request.params, Request.body)
   
   if v == false then
      net.http.respond{body=res, code=code}
      gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusInvalidMessage}
   else
      -- DO WORK
      local result = processRequest(Request.method, Request.params, Request.body)
      result = json.serialize{data=result, compact=false}
      net.http.respond{body=result,code=200,entity_type='application/json'}
      gsLog.log{logLevel=gsLog.level.Info, eventID=gsLogEvent.HttpStatusOk}
   end
   
   gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.CompletedMessageProcessing}
end

function processRequest(method, params, body)
   local rTab = {}
   rTab.Status = 'OK'
   if method == 'GET' then
      -- Do GET stuff
      if params.mode == 'getRule' then
         rTab.Action = 'getRule'
         if not params.Rule_GUID or params.Rule_GUID == '' then
            params.Rule_GUID = IRErs.getMessageTypeDetails{
               clientGUID=params.Client_GUID,
               formatType=params.Message_Format, 
               version=params.Message_Version, 
               messageCode=params.Message_Code}
            if #params.Rule_GUID > 0 then
               params.Rule_GUID = params.Rule_GUID[1].Rule_GUID
            end
         end
         rTab.Rule_GUID = params.Rule_GUID
         rTab.Payload = IRErs.getRuleAsTable{ruleGUID=params.Rule_GUID}
      end
      
   elseif method == 'POST' then
      -- Do POST stuff
      local body = json.parse{data=body}

      if body.IREmsh then
         rTab.Action = 'storeRule'
         rTab.Payload = {}
         --IRErs.storeRule({T}, limit1)
         rTab.Payload.IREmsh, rTab.Rule_GUID, rTab.InsertID = IRErs.storeRule({data=body}, false)
      elseif body.RuleCopy then
         rTab.Action = 'copyRule'
         rTab.Payload = {}
         --IRErs.copyRule({T}, limit1)
         rTab.Payload.IREmsh, rTab.Rule_GUID, rTab.InsertID, rTab.Removed_GUIDs = IRErs.copyRule({data=body}, false)
         --rule, RuleGUID, RuleID, removed
      elseif body.DuplicateFields then
         rTab.Action = 'duplicateRuleFields'
         rTab.Payload = {}
         --IRErs.storeRule({T}, limit1)
         rTab.Payload.IREmsh, rTab.Rule_GUID, rTab.InsertID = IRErs.duplicateRuleFields({data=body}, false)
      end
   end
   
   return rTab
end

function validateRequest(method, params, body)
   local s = true
   local res = nil
   local code = 200
   
   if method:upper() == 'GET' then
      -- Do GET stuff
      
      if not params.mode then
         s = false
         res = "REQUIRED PARAMETER MISSING when using GET method: mode"
         code=210
      elseif params.mode == 'getRule' then
         local rule
         if not (params.Rule_GUID and params.Rule_GUID ~= '') and not (params.Client_GUID and params.Client_GUID ~= '' and
            params.Message_Format and params.Message_Format ~= '' and
            params.Message_Version and params.Message_Version ~= '' and
            params.Message_Code and params.Message_Code ~= '') then
            
            s = false
            res = 'Unable to retrieve rule due to lack of parameters'
            code=210
         end
      else
         s = false
         res = "Unknown mode!"
         code=210
      end
   elseif method:upper() == 'POST' then
      -- Check validity of message
      if not body or body == '' then
         s = false
         res = "Body is empty! Must contain JSON directives..."
         code=210
      end
      local bol, res = pcall(json.parse, {data=body})
      if bol == false then
         s = false
         res = "Malformated JSON request!"
         code=210
      end
   end
   return s, res, code
end

--[[ LEGACY
   if Request.method == 'GET' then
      -- Do GET stuff
      local params = Request.get_params
      
      if not params.mode then
         net.http.respond{body="REQUIRED PARAMETER MISSING when using GET method: mode", code=210}
         gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusInvalidMessage}
      elseif params.mode == 'getRule' then
         local rule
         if params.Rule_GUID and params.Rule_GUID ~= '' then
            rule = IRErs.getRuleAsTable{ruleGUID=params.Rule_GUID}
            rule = json.serialize{data=rule}
            net.http.respond{body=rule,code=200,entity_type='application/json'}
            gsLog.log{logLevel=gsLog.level.Info, eventID=gsLogEvent.HttpStatusOk}
         elseif params.Client_GUID and params.Client_GUID ~= '' and
            params.Message_Format and params.Message_Format ~= '' and
            params.Message_Version and params.Message_Version ~= '' and
            params.Message_Code and params.Message_Code ~= '' then
            params.Rule_GUID = IRErs.getMessageTypeDetails{
               clientGUID=params.Client_GUID,
               formatType=params.Message_Format, 
               version=params.Message_Version, 
               messageCode=params.Message_Code}
            if #params.Rule_GUID > 0 then
               params.Rule_GUID = params.Rule_GUID[1].Rule_GUID
            end
            rule = IRErs.getRuleAsTable{ruleGUID=params.Rule_GUID}
            rule = json.serialize{data=rule}
            net.http.respond{body=rule,code=200,entity_type='application/json'}
            gsLog.log{logLevel=gsLog.level.Info, eventID=gsLogEvent.HttpStatusOk}
         else
            rule = 'Unable to retrieve rule due to lack of parameters'
            net.http.respond{body="Unknown mode!", code=210}
            gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusInvalidMessage}
         end
      else
         net.http.respond{body="Unknown mode!", code=210}
         gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusInvalidMessage}
      end
      
   elseif Request.method == 'POST' then
      -- Do POST stuff
      local s,body = pcall(json.parse, {data=body})

      -- Check validity of message
      if not body or body == '' then
         net.http.respond{body="Body is empty! Must contain JSON directives...",code=210}
         gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusInvalidMessage}
      elseif s == true then
         net.http.respond{body="OK",code=200}
         gsLog.log{logLevel=gsLog.level.Info, eventID=gsLogEvent.HttpStatusOk}
         queue.push{data=body}
      else
         net.http.respond{body=body,code=210}
         gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusInvalidMessage}
      end
   end
--]]

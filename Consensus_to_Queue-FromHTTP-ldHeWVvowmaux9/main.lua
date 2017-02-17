local Webpage  = require 'webpage'  -- Contains our default webpage html string.
require 'gsmos'
require 'gsLog'
require 'gsCrypt'
local InSchema = 'cts.vmd'

function main(Data)
   -- Initialize gsLog
   gsLog.Init{environment='p',logPrefix='Splunk'}
   gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.AnnounceMessageProcessing}
   --iguana.logInfo('PostData:\n\n'..Data)
   iguana.stopOnError(false)
   --local dta = gsCrypt.GetDecryptedData(Data)
   if Data ~= '' then
      -- Parse each incoming request with net.http.parseRequest
      local Request = net.http.parseRequest{data=Data}
      local bearer = Request.headers.bearer
      local msg = Request.params.message
      --local min,mt = hl7.parse{data=msg,vmd=InSchema}
   
      -- Authentication
      if bearer ~= gsmos.token then
         net.http.respond{body="Authentication Error",code=205}
         gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusAuthenticationError}
         return
      end
      -- Check validity of message
      if msg ~= '' and msg ~= nil then
         --local MsgIn,MsgType = hl7.parse{vmd=InSchema, data=msg}
         local s,e = pcall(hl7.parse,{vmd=InSchema, data=msg})
         if not s then
            net.http.respond{body="HL7 Invalid Format",code=215}
            gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusHl7InvalidFormat}
            return
         else
            --[[local evt = e.MSH[9][2]:nodeValue()
            if evt == 'A01' or 
            evt == 'A02' or 
            evt == 'A03' or
            evt == 'A04' then--]]
            -- Push the message to the queue
            local rspBody = {Status='OK', Request_Length=string.len(Data)}
            net.http.respond{body=json.serialize{data=rspBody, compact=true},code=200}
            gsLog.messageDetail{messageType=e.MSH[9][1]:nodeValue(),
               eventType=e.MSH[9][2]:nodeValue(),client_id=8}
            gsLog.message.hl7_message_id = e.MSH[10]:nodeValue()
            gsLog.message.message_source = 'golf101'
            gsLog.message.message_destination = 'iguana6'
            gsLog.log{logLevel=gsLog.level.Info, eventID=gsLogEvent.HttpStatusOk}
            queue.push{data=msg}
            gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.CompletedMessageProcessing}

            return
            --[[else
            net.http.respond{body="HL7 Not Supported Message",code=220}
            gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusHl7NotSupportedMessage}
            return
         end--]]
         end
      else
         net.http.respond{body="Invalid Message",code=210}
         gsLog.log{logLevel=gsLog.level.Warning, eventID=gsLogEvent.HttpStatusInvalidMessage}
         return
      end
   end
   gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.CompletedMessageProcessing}
end

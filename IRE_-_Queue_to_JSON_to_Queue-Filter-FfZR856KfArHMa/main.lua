require 'IRE_Processor'
require 'gsLog'

-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   gsLog.Init{environment=gsLog.environment.prod, logPrefix='Splunk'}
   
   local clientGUID = '00000000000000000000000000000001'
   local srcEndpointGUID = '000_FAKE-DB-SOURCE_000'
   local dstEndpointGUID = '000_FAKE-DB-DEST_000'
   local srcFormat = 'HL7'
   local srcVersion = 'cts'
   local Client_ID = 8
   
   local fVmd = 'HL7_'..srcVersion..'.vmd'
   if srcVersion:lower() == 'cts' then
      fVmd = 'cts.vmd'
   end
   local srcMsg = hl7.parse{vmd=fVmd, data=Data}
   
   gsLog.messageDetail{client_id=Client_ID, messageType=srcMsg.MSH[9][1]:nodeValue(),
      eventType=srcMsg.MSH[9][2]:nodeValue(),patient_id=srcMsg.MSH[10]:nodeValue()}
   gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.AnnounceMessageProcessing}
   
   --iguana.logInfo("Inbound HL7:\r\n"..Data)
   
   local msgTypeDetails = IRErs.getMessageTypeDetails{
      clientGUID=clientGUID,
      formatType=srcFormat, 
      version=srcVersion, 
      messageCode=srcMsg:nodeName()}
   
   if not msgTypeDetails then
      iguana.logWarning("Unfamiliar message type... \r\n"..
         "Need to generate a standard rule based on:\r\n"..
         "Format Type: "..srcFormat.."\r\n"..
         "Version: "..srcVersion.."\r\n"..
         "Message Code: "..srcMsg:nodeName())
      return
   end
   
   IREcustfld.Client_ID = Client_ID
   IREcustfld.SQLtype = db.MY_SQL
   
   local flowDetail = IRErs.getFlowDetails{srcRuleGUID=msgTypeDetails[1].Rule_GUID, 
      srcEndpointGUID=srcEndpointGUID, dstEndpointGUID=dstEndpointGUID} 
   
   if flowDetail then
      flowDetail.data = srcMsg

      local result = IREproc.flowToDataTable(flowDetail)
      iguana.logInfo("Outbound JSON:\r\n"..json.serialize{data=result})
      --trace(json.serialize{data=result})
   else
      iguana.logWarning("No RuleStore flow record for this message type or endpoints...\r\n"..
      "Message_Code:"..srcMsg:nodeName())
   end
   
   gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.CompletedMessageProcessing}
end
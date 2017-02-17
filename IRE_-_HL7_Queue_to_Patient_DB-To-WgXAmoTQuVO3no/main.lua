require 'IRE_Processor'
require 'gsLog'

-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   gsLog.Init{environment=gsLog.environment.prod,logPrefix='Splunk'}
   local clientGUID = '00000000000000000000000000000001'
   local srcEndpointGUID = '000_FAKE-DB-SOURCE_000'
   local dstEndpointGUID = '000_FAKE-DB-DEST_000'
   local srcFormat = 'HL7'
   local srcVersion = '2.3'
   local Client_ID = 8
   
   local srcMsg = hl7.parse{vmd='HL7_'..srcVersion..'.vmd', data=Data}
   
   gsLog.messageDetail{client_id=Client_ID, messageType=srcMsg.MSH[9][1]:nodeValue(),
      eventType=srcMsg.MSH[9][2]:nodeValue(),patient_id=srcMsg.MSH[10]:nodeValue()}
   gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.AnnounceMessageProcessing}
   
   
   local msgTypeDetails = IRErs.getMessageTypeDetails{
      clientGUID=clientGUID,
      formatType=srcFormat,
      version=srcVersion,
      messageCode=srcMsg:nodeName()}
   
   IREcustfld.Client_ID = Client_ID
   IREcustfld.SQLtype = db.MY_SQL
   
   local flowDetail = IRErs.getFlowDetails{
      srcRuleGUID=msgTypeDetails[1].Rule_GUID,
      srcEndpointGUID=srcEndpointGUID,
      dstEndpointGUID=dstEndpointGUID}
   flowDetail.data = srcMsg
   
   local result = IREproc.flowToDataTable(flowDetail)
   trace(json.serialize{data=result})
   gsLog.log{logLevel=gsLog.level.Info,eventID=gsLogEvent.CompletedMessageProcessing}
end
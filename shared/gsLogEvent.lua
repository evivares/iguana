gsLogEvent = {}

--[[
All event ID's should include an error code and a wiki link with a description of the error code as well as some troubleshooting techniques
]]--

--[[ 10000 - 19999 for Info type events ]]--
-- Description: Announce start of message processing 
gsLogEvent.AnnounceMessageProcessing = '10001'

-- Description: Completed message processing
gsLogEvent.CompletedMessageProcessing = '10002'

-- Description: Received a message of an unexpected type for this channel
gsLogEvent.UnexpectedMessageType = '10003'

-- Description: Generic Function Begin - Marker for debugging
gsLogEvent.FunctionBegin = '10004'

-- Description: Generic Function End - Marker for debugging
gsLogEvent.FunctionEnd = '10005'

--[[ 20000 - 29999 for Warning type events]]--
-- Description: Generic application alert/warning. This event ID can be used if there is no defined alert/warning yet especially during development
gsLogEvent.ApplicationAlert = '20001'

--[[ 30000 - 39999 for Error type events ]]--
-- Description: Required Segment Missing: MSH
gsLogEvent.RequiredMSHSegmentMissing = '30001'

-- Description of the below Event ID with wiki link
gsLogEvent.HttpStatusOk = '200'

-- Description of the below Event ID with wiki link
gsLogEvent.HttpStatusAuthenticationError = '205'

-- Description of the below Event ID with wiki link
gsLogEvent.HttpStatusInvalidMessage = '210'

-- Description of the below Event ID with wiki link
gsLogEvent.HttpStatusHl7InvalidFormat = '215'

-- Description of the below Event ID with wiki link
gsLogEvent.HttpStatusHl7NotSupportedMessage = '220'

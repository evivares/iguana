require 'amplefi'
require 'gsmos_order'
require 'gsmos_screening'
require 'gsmos_patient'
require 'gsmos_hl7message'
require 'gsmos_visit'
require 'gsmos_appointment'
require 'gsmos_results'
require 'gsLog'
-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   gsLog.Init{environment=gsLog.environment.prod,logPrefix='Splunk'}
   gsLog.log{logLevel=gsLog.level.Info, eventID=gsLogEvent.AnnounceMessageProcessing}

   iguana.stopOnError(false)
   
   local MsgIn,MsgType = hl7.parse{data=Data,vmd='cts.vmd'}
   local EventType = MsgIn.MSH[9][2]:nodeValue()
   local cid = 8  -- Client ID
   local mcid = cid..'*'..MsgIn.MSH[3][1]..'*'..MsgIn.MSH[4][1]..'*'
   mcid = mcid..MsgIn.MSH[5][1]..'*'..MsgIn.MSH[7][1]..'*'..MsgIn.MSH[9][1]
   mcid = mcid..'*'..MsgIn.MSH[9][2]..'*'..MsgIn.MSH[10]..'*'..MsgIn.MSH[11][1]
   mcid = mcid..'*'..MsgIn.MSH[12][1]
   --iguana.logInfo('Success:\n\n'..mcid)

   --[[
   gsLog.messageDetail{messageType=MsgIn.MSH[9][1]:nodeValue(),
      eventType=MsgIn.MSH[9][2]:nodeValue(),client_id=cid}
   gsLog.message.message_source = 'golf101'
   gsLog.message.message_destination = 'iguana6'
   gsLog.message.hl7_message_id = MsgIn.MSH[10]:nodeValue()
   --]]

   
   --[[
   local ln,fn,dob = '','',''
   local patientExists = false
   
   if MsgType == 'SIU' then
      ln = MsgIn.PATIENT.PID[1][5][1][1][1]
      fn = MsgIn.PATIENT.PID[1][5][1][2]
      dob = MsgIn.PATIENT.PID[1][7][1]
   elseif MsgType == 'ORU_SCREEN' then
      ln = MsgIn.RESPONSE[1].PATIENT.PID[5][1][1][1]
      fn = MsgIn.RESPONSE[1].PATIENT.PID[5][1][2]
      dob = MsgIn.RESPONSE[1].PATIENT.PID[7][1]
   else
      ln = MsgIn.PATIENT.PID[5][1][1][1]
      fn = MsgIn.PATIENT.PID[5][1][2]
      dob = MsgIn.PATIENT.PID[7][1]
   end
   
   local guid = gsmos.getPatient(ln,fn,dob)
   if guid == '' then
      guid = util.guid(512)
   else
      patientExists = true
   end
   
   gsmos.saveMessage(MsgIn,mcid,cid)
   
   if not patientExists then
      -- New patient insert
      if MsgType == 'SIU' then
         gsmos.processPatient(MsgIn.PATIENT.PID[1],guid,mcid)
      elseif MsgType == 'ORU_SCREEN' then
         gsmos.processPatient(MsgIn.RESPONSE[1].PATIENT.PID,guid,mcid)
      else
         gsmos.processPatient(MsgIn.PATIENT.PID,guid,mcid)
      end
   end
   
   local pv1x,pv1y = Data:find('PV1|')
   if pv1x ~= nil then
      if EventType == 'A01' or
         EventType == 'A04' then
         -- Insert new Visit
         gsmos.processVisit(MsgIn["PATIENT VISIT"].PV1,guid,mcid)
      elseif EventType == 'A03' then
         -- Patient Discharge
         -- Do we insert a record in the visit table?
         -- Or, do we just update an existing visit with the discharge info?
      elseif EventType == 'A05' then
         -- Pre-admit patient
         -- Do we care about pre-admissions?
      elseif EventType == 'A02' or
         EventType == 'A06' or
         EventType == 'A07' then
         -- Patient Transfer
         -- Do we insert a record in the visit table?
         -- Or, do we just update an existing visit with the discharge info?
      elseif EventType == 'A08' then
         -- Update Patient Information
         -- Do we insert a record in the visit table?
         -- Or, do we just update an existing visit with the discharge info?
      elseif MsgType == 'ORU_SCREEN' then
         gsmos.processVisit(MsgIn.RESPONSE[1].PATIENT["PATIENT VISIT"].PV1,guid,mcid)
      elseif MsgType == 'SIU' then
         -- Do not load PV1 segment
      else
         gsmos.processVisit(MsgIn["PATIENT VISIT"].PV1,guid,mcid)
      end
   end
   
   trace(MsgIn)
      
   if MsgType == 'SIU' then
      if EventType == 'S12' then
         -- New patient appointment
         gsmos.processAppointment(MsgIn.SCH,guid,mcid)
         local anotes = MsgIn.NTE
         for i = 1, #anotes do
            gsmos.processAppointmentNotes(anotes[i],guid,mcid)
         end
      elseif EventType == 'S13' then
         -- Appointment Rescheduling
         gsmos.processAppointmentChanged(MsgIn.SCH,guid,mcid,EventType)
      elseif EventType == 'S14' then
         -- Appointment Modification
         gsmos.processAppointmentChanged(MsgIn.SCH,guid,mcid,EventType)
      elseif EventType == 'S15' then
         -- Appointment Cancellation
         gsmos.processAppointmentChanged(MsgIn.SCH,guid,mcid,EventType)
      elseif EventType == 'S16' then
         -- Appointment Discontinuation
         gsmos.processAppointmentChanged(MsgIn.SCH,guid,mcid,EventType)
      elseif EventType == 'S26' then
         -- Appointment No Show
         gsmos.processAppointmentChanged(MsgIn.SCH,guid,mcid,EventType)
      else
         -- Unhandled event
          gsmos.processAppointmentChanged(MsgIn.SCH,guid,mcid,EventType)
      end
   elseif MsgType == 'ORU_LAB' then
      gsmos.processLabOrder(MsgIn.ORDER,guid,mcid)
   elseif MsgType == 'ORU_SCREEN' then
      gsmos.processScreenings(MsgIn.RESPONSE[1].ORDER,guid,mcid)
   elseif MsgType == 'ADT' then
   elseif MsgType == 'PPR' then
   elseif MsgType == 'ORM' then
   elseif MsgType == 'RDE' then
      
   end
   --]]
   
   gsLog.log{logLevel=gsLog.level.Info, eventID=gsLogEvent.CompletedMessageProcessing}
end

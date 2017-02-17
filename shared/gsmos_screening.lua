require 'gsmos'

function gsmos.processScreenings(ORDER,guid,mcid)
   for i = 1, #ORDER do
      -- Screening header
      gsmos.processScreenHeader(ORDER,i,guid,mcid)
      -- Screening details
      local obxx,obxy = tostring(ORDER[i]):find('OBX|')
      if obxx ~= nil then
         local obs = ORDER[i].OBSERVATION
         for j = 1, #obs do
            gsmos.processScreenDetails(obs,i,j,guid,mcid)
         end
      end
   end
end

function gsmos.processScreenHeader(o,i,guid,mcid)
   -- (1) connect to the database
   if not Conn or not Conn:check() then
      Conn = db.connect{
         api=db.SQL_SERVER,
         name=amplefi.mssqlRdsServer,
         user=amplefi.mssqlRdsUname,
         password=amplefi.mssqlRdsPword,
         live=true
      }
   end
   trace(o[i].OBR[25]:nodeValue())

   -- (2) create insert query string
   local SqlInsert =
   [[
   INSERT INTO cchhs.dbo.screen_header
   (
   GUID,
   MsgID,
   OrderControl,
   SetID,
   FillerOrderNumber,
   ServiceIdCode,
   ServiceIdText,
   AlternateIdCode,
   AlternateCodingSystem,
   ObservationDateTime,
   ObservationEndDateTime,
   FillerField2,
   StatusChangeDateTime,
   DiagnosticServSectId,
   ResultStatus,
   InterpreterID,
   InterpreterLastName,
   InterpreterFirstName,
   TechnicianID,
   TechnicianLastName,
   TechnicianFirstName
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   '"..o[i].ORC[1]:nodeValue().."',"..
   "\n   '"..o[i].OBR[1]:nodeValue().."',"..
   "\n   '"..o[i].OBR[3][1]:nodeValue().."',"..
   "\n   '"..amplefi.DbValue(o[i].OBR[4][1]:nodeValue()).."',"..
   "\n   '"..amplefi.DbValue(o[i].OBR[4][2]:nodeValue()).."',"..
   "\n   '"..amplefi.DbValue(o[i].OBR[4][4]:nodeValue()).."',"..
   "\n   '"..amplefi.DbValue(o[i].OBR[4][5]:nodeValue()).."',"..
   "\n   '"..o[i].OBR[7]:nodeValue().."',"..
   "\n   '"..o[i].OBR[8]:nodeValue().."',"..
   "\n   '"..o[i].OBR[21]:nodeValue().."',"..
   "\n   '"..o[i].OBR[22]:nodeValue().."',"..
   "\n   '"..o[i].OBR[24]:nodeValue().."',"..
   "\n   '"..o[i].OBR[25]:nodeValue().."',"..
   "\n   '"..o[i].OBR[32][1][1]:nodeValue().."',"..
   "\n   '"..amplefi.DbValue(o[i].OBR[32][1][2]:nodeValue()).."',"..
   "\n   '"..amplefi.DbValue(o[i].OBR[32][1][3]:nodeValue()).."',"..
   "\n   '"..o[i].OBR[34][1][1][1]:nodeValue().."',"..
   "\n   '"..amplefi.DbValue(o[i].OBR[34][1][1][2]:nodeValue()).."',"..
   "\n   '"..amplefi.DbValue(o[i].OBR[34][1][1][3]:nodeValue()).."'"..
   "\n   )"  
   
   -- (3) Insert data into database
   if not iguana.isTest() then
      Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
   end
   
end

function gsmos.processScreenDetails(o,i,j,guid,mcid)
   -- (1) connect to the database
   if not Conn or not Conn:check() then
      Conn = db.connect{
         api=db.SQL_SERVER,
         name=amplefi.mssqlRdsServer,
         user=amplefi.mssqlRdsUname,
         password=amplefi.mssqlRdsPword,
         live=true
      }
   end
   --trace(o[j].OBX[16][1][2][1]:nodeValue())
	
   -- (2) create insert query string
   local SqlInsert =
   [[
   INSERT INTO cchhs.dbo.screen_detail
   (
   GUID,
   MsgID,
   HeaderSetID,
   DetailSetID,
   ValueType,
   DetailIdCode,
   DetailIdText,
   Value,
   ObservationResultStatus,
   ObservationDateTime,
   ResponsibleObserverID,
   ResponsibleObserverLastName,
   ResponsibleObserverFirstName
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   '"..i.."',"..
   "\n   '"..j.."',"..
   "\n   '"..o[j].OBX[2]:nodeValue().."',"..
   "\n   '"..amplefi.DbValue(o[j].OBX[3][1]:nodeValue()).."',"..
   "\n   '"..amplefi.DbValue(o[j].OBX[3][2]:nodeValue()).."',"..
   "\n   '"..amplefi.DbValue(o[j].OBX[5][1][1][1]:nodeValue()).."',"..
   "\n   '"..o[j].OBX[11]:nodeValue().."',"..
   "\n   '"..o[j].OBX[14]:nodeValue().."',"..
   "\n   '"..o[j].OBX[16][1][1]:nodeValue().."',"..
   "\n   '"..amplefi.DbValue(o[j].OBX[16][1][2][1]:nodeValue()).."',"..
   "\n   '"..amplefi.DbValue(o[j].OBX[16][1][3]:nodeValue()).."'"..
   "\n   )"  
	
   -- (3) Insert data into database
   if not iguana.isTest() then
      -- Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
   end
end
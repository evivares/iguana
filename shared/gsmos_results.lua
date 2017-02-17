require 'gsmos'

function gsmos.processLabOrder(ORDER,guid,mcid)
   local otype = ORDER[1].ORDER_DETAIL[1].OBR[24]:nodeValue()
   for i = 1, #ORDER do
      gsmos.processLabCommonOrder(ORDER[i].ORC,guid,mcid)
      gsmos.processLabOrderRequest(ORDER[i].ORDER_DETAIL[1].OBR,guid,mcid)
      gsmos.processLabResults(ORDER[i],guid,mcid)
   end
end

function gsmos.processLabCommonOrder(ORC,guid,mcid)
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
   
   -- (2) create insert query string
   local SqlInsert =
   [[
   INSERT INTO cchhs.dbo.order
   (
   GUID,
   MsgID,
   OrderControl,
   PlacerOrderNumber,
   FillerOrderNumber,
   TransactionDateTime,
   OrderingProviderId
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   '"..ORC[1].."',"..
   "\n   '"..ORC[2][1].."',"..
   "\n   '"..ORC[3][1].."',"..
   "\n   '"..ORC[9].."',"..
   "\n   '"..ORC[12][1][1].."'"..
   '\n   )'  
   
   if not iguana.isTest() then
      Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
   end

end

function gsmos.processLabOrderRequest(OBR,guid,mcid)
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
   
   -- (2) create insert query string
   local SqlInsert =
   [[
   INSERT INTO cchhs.dbo.order_request
   (
   GUID,
   MsgID,
   SetID,
   PlacerOrderNumber,
   FillerOrderNumber,
   ServiceIdCode,
   ServiceIdText,
   ObservationDateTime,
   CollectorId,
   SpecimenReceiveDateTime,
   OrderingProviderId,
   OrderingProviderLastName,
   OrderingProviderFirstName,
   FillerField2,
   DiagnosticServSectId,
   ResultStatus,
   StartDateTime,
   Priority,
   Parent
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   '"..OBR[1].."',"..
   "\n   '"..OBR[2][1].."',"..
   "\n   '"..OBR[3][1].."',"..
   "\n   '"..OBR[4][1].."',"..
   "\n   '"..OBR[4][2].."',"..
   "\n   '"..OBR[7].."',"..
   "\n   '"..OBR[10][1][1].."',"..
   "\n   '"..OBR[14].."',"..
   "\n   '"..OBR[16][1][1].."',"..
   "\n   '"..OBR[16][1][2][1].."',"..
   "\n   '"..OBR[16][1][3].."',"..
   "\n   '"..OBR[21].."',"..
   "\n   '"..OBR[22].."',"..
   "\n   '"..OBR[24].."',"..
   "\n   '"..OBR[25].."',"..
   "\n   '"..OBR[27][1][4].."',"..
   "\n   '"..OBR[29][1][1].."'"..
   '\n   )'  
   
   if not iguana.isTest() then
      Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
   end

end

function gsmos.processLabResults(ORDER,guid,mcid)
   local results = ORDER.ORDER_DETAIL[1].OBSERVATION
   for j = 1, #results do
      local ontex,ontey = (tostring(results[j]):find('NTE'))
      if ontex ~= nil then
         gsmos.processResultNotes(results[j].NTE,guid,mcid,j)
      end
      gsmos.processResult(results[j].OBX,guid,mcid)
   end
end

function gsmos.processResultNotes(NTE,guid,mcid,rsid)
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
   -- (2) create insert query string
   for i = 1, #NTE do
      trace(amplefi.DbValue(tostring(NTE[i][3][1])))
      local SqlInsert =
      [[
      INSERT INTO cchhs.dbo.result_note
      (
      [GUID]
      ,[MsgID]
      ,[ResultSetID]
      ,[ResultNoteSetID]
      ,[ResultNote]
      )
      VALUES
      (
      ]]..
      "'"..guid.."',"..   -- GUID
      "\n   '"..mcid.."',"..   -- MsgID
      "\n   '"..rsid.."',"..   -- ResultSetID
      "\n   '"..NTE[i][1].."',"..   -- ResultNoteSetID
      "\n   '"..amplefi.DbValue(tostring(NTE[i][3][1])).."'"..   -- ResultNote
      '\n   )'  
   
      trace(SqlInsert)
      -- (3) Insert data into database
      -- Conn:execute{sql=SqlInsert, live=true}
   end
end

function gsmos.processResult(OBX,guid,mcid)
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
   
   -- (2) create insert query string
   local SqlInsert =
   [[
   INSERT INTO cchhs.dbo.result
   (
   GUID,
   MsgID,
   SetID,
   ValueType,
   ResultIdCode,
   ResultIdText,
   AlternateIdCode,
   AlternateCodingSystem,
   Value,
   Units,
   ReferenceRange,
   Status,
   ObservationDateTime,
   ResponsibleObserver
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   '"..OBX[1].."',"..
   "\n   '"..OBX[2].."',"..
   "\n   '"..OBX[3][1].."',"..
   "\n   '"..OBX[3][2].."',"..
   "\n   '"..OBX[3][4].."',"..
   "\n   '"..OBX[3][6].."',"..
   "\n   '"..OBX[5][1][1][1].."',"..
   "\n   '"..OBX[6][1].."',"..
   "\n   '"..OBX[7].."',"..
   "\n   '"..OBX[11].."',"..
   "\n   '"..OBX[14].."',"..
   "\n   '"..OBX[16][1][1].."'"..
   '\n   )'  
   
   trace(SqlInsert)
   -- (3) Insert data into database
   -- Conn:execute{sql=SqlInsert, live=true}
end

require 'gsmos'

function gsmos.processAppointmentChanged(SCH,guid,mcid,evt)
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
   local status = ''
   if evt == 'S13' then
      status = 'Rescheduled'
   elseif evt == 'S14' then
      status = 'Modified'
   elseif evt == 'S15' then
      status = 'Cancelled'
   elseif evt == 'S16' then
      status = 'Discontinued'
   elseif evt == 'S17' then
      status = 'Deleted'
   elseif evt == 'S26' then
      status = 'No Show'
   else
      status = 'Unhandled - '..evt
   end
   local sql = "UPDATE cchhs.dbo.appointment SET "
   sql = sql.."\n  MsgID = '"..mcid.."'"
   sql = sql.."\n  ,AppointmentReason = '"..amplefi.DbValue(SCH[7][1]:nodeValue()).."'"
   sql = sql.."\n  ,Duration = '"..SCH[9]:nodeValue().."'"
   sql = sql.."\n  ,DurationUnits = '"..SCH[10][1]:nodeValue().."'"
   sql = sql.."\n  ,StartDateTime = '"..SCH[11][4]:nodeValue().."'"
   sql = sql.."\n  ,EndDateTime = '"..SCH[11][5]:nodeValue().."'"
   sql = sql.."\n  ,Status = '"..status.."'"
   sql = sql.."\n  ,EnteredBy = '"..SCH[20][1][1].."'"
   sql = sql.."\n WHERE GUID = '"..guid.."'"
   sql = sql.."\n AND PlacerID = '"..SCH[1][1]:nodeValue().."'"
   
   if not iguana.isTest() then
      Conn:execute{sql=sql, live=true}
   else
      trace(sql)
   end

end

function gsmos.processAppointment(SCH,guid,mcid)
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
   INSERT INTO cchhs.dbo.appointment
   (
   GUID,
   MsgID,
   PlacerID,
   AppointmentReason,
   Duration,
   DurationUnits,
   StartDateTime,
   EndDateTime,
   Status,
   EnteredBy
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   '"..SCH[1][1].."',"..
   "\n   '"..SCH[7][1].."',"..
   "\n   '"..SCH[9].."',"..
   "\n   '"..SCH[10][1].."',"..
   "\n   '"..SCH[11][4].."',"..
   "\n   '"..SCH[11][5].."',"..
   "\n   '"..SCH[25][1].."',"..
   "\n   '"..SCH[20][1][1].."'"..
   '\n   )'  
   
   if not iguana.isTest() then
      Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
   end

end

function gsmos.processAppointmentNotes(NTE,guid,mcid)
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
   INSERT INTO cchhs.dbo.appointment_note
   (
   GUID,
   MsgID,
   SetID,
   SourceOfComment,
   Comment
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   '"..NTE[1].."',"..
   "\n   '"..NTE[2].."',"..
   "\n   '"..NTE[3][1].."'"..
   '\n   )'  

   if not iguana.isTest() then
      Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
   end

end


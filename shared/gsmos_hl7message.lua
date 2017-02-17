require 'gsmos'

function gsmos.saveMessage(msg,mcid,cid)
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
   trace(msg.MSH[9][1])
   -- (2) create insert query string
   local SqlInsert =
   [[
   INSERT INTO cchhs.dbo.hl7_message
   (
   [MsgID]
   ,[ClientID]
   ,[MessageType]
   ,[EventType]
   ,[MessageControlID]
   ,[SendingApplication]
   ,[SendingFacility]
   ,[ReceivingApplication]
   ,[ReceivingFacility]
   ,[MessageTimestamp]
   ,[LoadDateTime]
   ,[MessageRaw]
   )
   VALUES
   (
   ]]..
   "'"..mcid.."',"..
   "\n   "..cid..","..
   "\n   '"..msg.MSH[9][1].."',"..
   "\n   '"..msg.MSH[9][2].."',"..
   "\n   '"..msg.MSH[10].."',"..
   "\n   '"..msg.MSH[3][1].."',"..
   "\n   '"..msg.MSH[4][1].."',"..
   "\n   '"..msg.MSH[5][1].."',"..
   "\n   '"..msg.MSH[6][1].."',"..
   "\n   '"..msg.MSH[7][1].."',"..
   "\n   getdate(),"..
   "\n   '"..amplefi.DbValue(tostring(msg)).."'"..
   '\n   )'  
   
   -- (3) Insert data into database
   if not iguana.isTest() then
      Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
   end
   
end


require 'amplefi'
require 'stringutil'
-- The main function is the first function called from Iguana.
function main()
   if not iguana.isTest() then
      iguana.setTimeout(6400)
   end
   --getHecActiveUsers()
   --removeInactiveUsersFromStatusTable()
   --getHecUserStatusData()
   queueInsertUserStatus()
end

function queueInsertUserStatus()
   iguana.logInfo('Start InsertHecUserStatusData()')
   --local sql = "TRUNCATE TABLE ecompass.dbo.user_status"
   --local ret = amplefi.ExecuteMsSql(sql)
   local dt = os.date('*t')
   local dts = dt.month..'/'..dt.day..'/'..dt.year..' '..dt.hour..':'..dt.min..':'..dt.sec
   local sql = "SELECT user_key,LEFT(user_key,3) AS client_id,user_first_name,user_socket_id FROM auth.users " 
   sql = sql.."WHERE LEFT(user_key,3) IN ('acs','hmk','ahn') "
   if iguana.isTest() then
      sql = sql.."LIMIT 10"
   end
   local ret = amplefi.ExecuteMySql(sql)
   local jsn = ''
   local pos = ''
   if #ret > 0 then
      for i = 1, #ret do
         sql = "INSERT INTO ecompass.dbo.user_status (addedon,user_key,client_code,user_first_name,user_socket_id,[partial]) "
         sql = sql.."VALUES ('"..dts.."','"..amplefi.GetEncryptedData(ret[i].user_key:nodeValue()).."','"..ret[i].client_id:nodeValue().."','"
         if ret[i].user_first_name:nodeValue() ~= ' ' then
            sql = sql..amplefi.GetEncryptedData(ret[i].user_first_name:nodeValue())
         end
         sql = sql.."','"..ret[i].user_socket_id:nodeValue().."','Y');"
         if i == 1 then
            pos = 'start'
         elseif i == #ret then
            pos = 'end'
         else
            pos = 'middle'
         end
         jsn = 'user_status|'..i..'|'..sql..'|'..pos
         trace(jsn)
         queue.push{data=jsn}
      end
   end
   iguana.logInfo('End InsertHecUserStatusData()')
end
--[[
function getHecUserStatusData()
   iguana.logInfo('Start getHecUserStatusData()')
   --local sql = "TRUNCATE TABLE ecompass.dbo.user_status"
   --local ret = amplefi.ExecuteMsSql(sql)
   local sql = 'SELECT user_key,client_id,user_status FROM auth.vw_ecompass_user_stats' 
   local ret = amplefi.ExecuteMySql(sql)
   local jsn = ''
   if #ret > 0 then
      for i = 1, #ret do
         jsn = '{"table":"user_status","user_key":"'..ret[i].user_key..'","client_code":"'
         jsn = jsn..ret[i].client_id..'","user_status":"'..ret[i].user_status..'"}'
         --queue.push{data=tostring(jsn)}
      end
   end
   iguana.logInfo('End getHecUserStatusData()')
end

function getHecActiveUsers()
   iguana.logInfo('Start getHecActiveUsers()')
   local sql = "TRUNCATE TABLE ecompass.dbo.tmpActiveUsersList"
   --local ret = amplefi.ExecuteMsSql(sql)
   sql = "SELECT user_key FROM auth.users"
   ret = amplefi.ExecuteMySql(sql)
   local jsn = ''
   for i = 1, #ret do
      jsn = '{"table":"tmpActiveUsersList","user_key":"'..ret[i].user_key..'"}'
      queue.push{data=tostring(jsn)}
   end
   iguana.logInfo('End getHecActiveUsers()')
end

function removeInactiveUsersFromStatusTable()
   iguana.logInfo('Start removeInactiveUsersFromStatusTable()')
   local sql = "DELETE FROM ecompass.dbo.user_status WHERE user_key NOT "
   sql = sql.."IN (SELECT user_key FROM ecompass.dbo.tmpActiveUsersList)"
   local ret = amplefi.ExecuteMsSql(sql)
   iguana.logInfo('End removeInactiveUsersFromStatusTable()')
end
--]]
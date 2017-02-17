require 'amplefi'
require 'stringutil'
-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   if Data ~= '' then
      local dt = Data:split('|')
      trace(dt[4])
      if dt[1] == 'user_status' then
         --processHecUserStatus(jsn)
         insertUserStatus(dt[3])
      --elseif jsn.table == 'tmpActiveUsersList' then
         --processTmpActiveUsersList(jsn)
      else
      end
      if dt[4] == 'end' then
         if not iguana.isTest() then
            local sql = 'exec amp.dev.build_ecompass_user_stats_table'
            local ret = amplefi.ExecuteMsSqlDb(sql,'ecompass')
            --trace(ret)
         end
      end
   end
end

function insertUserStatus(sql)
   if not iguana.isTest() then
      iguana.stopOnError(false)
      local rec = amplefi.ExecuteMsSql(sql)
   end
end
--[[
function processHecUserStatus(jsn)
   --local uk,cc,us = getHecUserStatus(jsn.user_key)
   local sql = ''
   local dt = os.date('*t')
   local dts = dt.month..'/'..dt.day..'/'..dt.year..' '..dt.hour..':'..dt.min..':'..dt.sec
   --if uk ~= '' then
      -- Update
   --   if jsn.user_status ~= us then
   --      sql = "UPDATE ecompass.dbo.user_status SET "
   --      sql = sql.."user_status = '"..us.."' WHERE "
   --      sql = sql.."user_key = '"..uk.."'"
   --   end
   --else
      -- Insert
      sql = "INSERT INTO ecompass.dbo.user_status (addedon,user_key,client_code,user_status) "
      sql = sql.."VALUES ('"..dts.."','"..jsn.user_key.."','"..jsn.client_code.."','"..jsn.user_status.."')"
   --end
   if sql ~= '' then
      local ret = amplefi.ExecuteMsSql(sql)
   end
end

function processTmpActiveUsersList(jsn)
   local sql = "INSERT INTO ecompass.dbo.tmpActiveUsersList (user_key) "
   sql = sql.."VALUES ('"..jsn.user_key.."')"
   local ret = amplefi.ExecuteMsSql(sql)
end

function getHecUserStatus(ukey)
   local sql = "SELECT * FROM ecompass.dbo.user_status WHERE user_key = '"..ukey.."'"
   local ret = amplefi.ExecuteMsSql(sql)
   local uk,cc,us = '','',''
   if #ret > 0 then
      uk = tostring(ret[1].user_key)
      cc = tostring(ret[1].client_code)
      us = tostring(ret[1].user_status)
   end
   return uk,cc,us
end
--]]
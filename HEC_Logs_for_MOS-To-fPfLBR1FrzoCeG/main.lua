require('amplefi')
-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   iguana.stopOnError(false)
   if Data ~= '' then
      local jsn = json.parse{data=Data}
      if timeblockExists(jsn.ts) then
         -- update
         --UpdatePerfLog(jsn)
      else
         -- insert
         InsertPerfLog(jsn)
      end
   end
end

function timeblockExists(ts)
   local tmp = false
   local sql = "SELECT time_stamp FROM ecompass.dbo.perf_log WHERE time_stamp = '"..ts.."'"
   local rec = amplefi.ExecuteMsSql(sql)
   if #rec > 0 then
      tmp = true
   end
   return tmp
end

function InsertPerfLog(jsn)
   local kvs = amplefi.DbValue(json.serialize{data=jsn.kv})
   local usr = amplefi.DbValue(jsn.user)
   if usr ~= '' then
      usr = amplefi.GetEncryptedData(usr)
   end
   local sql = "INSERT INTO ecompass.dbo.perf_log ([time_stamp],"
--   sql = sql.."[service],[server],[device],[execution_time],[user],[user_ip],[client_code],"
   sql = sql.."[service],[device],[user],[user_ip],[client_code],"
   sql = sql.."[key],[kvs_key],[kvs_value],[session_data]) VALUES ('"..jsn.ts.."','"..jsn.srvc
--   sql = sql.."','"..jsn.srvr.."','"..jsn.dv.."','"..jsn.etime.."','"..usr.."','"
   sql = sql.."','"..jsn.dv.."','"..usr.."','"
   sql = sql..jsn.uip.."','"..jsn.cid:gsub('"','').."','"..jsn.k.."','"..jsn.kvk
   sql = sql.."','"..jsn.kv.storyName.."','"..jsn.session_data.."');"
   --trace(sql)
   local result = amplefi.ExecuteMsSql(sql)
end

function UpdatePerfLog(jsn)
   --trace(jsn.kv.storyName)
   local sql = "UPDATE ecompass.dbo.perf_log SET session_data = '"..jsn.session_data.."' "
   sql = sql.."WHERE [time_stamp] = '"..jsn.ts.."';"
   local result = amplefi.ExecuteMsSql(sql)
end
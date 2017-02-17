require 'amplefi'
require 'stringutil'
require 'urlcode'

-- The main function is the first function called from Iguana.
function main()
   -- Keep this channel running even in the event of error
   
   if not iguana.isTest() then
      iguana.stopOnError(false)
      iguana.setTimeout(1600)
   end
   --LogsForMOS()
end

function getMaxTimeStamp()
   local sql = "SELECT MAX(time_stamp) AS ts FROM ecompass.dbo.user_activity"
   local rec = amplefi.ExecuteMsSql(sql)
   return tostring(rec[1].ts:nodeValue())
end

function LogsForMOS()
   local maxts = getMaxTimeStamp()
   --maxts = '2016-01-25_17:55:27.1453744527'
   local sql = "SELECT time_stamp,service,clientid,userid,`key`,"
   sql = sql.."user_activity,device,item_name,companyname,"
   sql = sql.."contact_pref,dob,gender,location "
   sql = sql.."FROM performance.vwEcompassUserActivities "
   sql = sql.."WHERE time_stamp > '"..maxts.."' "
   --sql = sql.."AND companyname <> '' "
   --sql = sql.."AND clientid = 'acs' "
   sql = sql.."ORDER BY time_stamp "
   if iguana.isTest() then
      sql = sql.."LIMIT 1"
   else
      sql = sql.."LIMIT 10000"
   end
   local ret = amplefi.ExecuteMySql(sql)
   local qd = ''     -- json to be queued
   for i = 1, #ret do
      qd = '{"time_stamp":"'..ret[i].time_stamp:nodeValue()..'",'
      qd = qd..'"service":"'..ret[i].service:nodeValue()..'",'
      qd = qd..'"clientid":"'..ret[i].clientid:nodeValue()..'",'
      qd = qd..'"userid":"'..ret[i].userid:nodeValue()..'",'
      qd = qd..'"key":"'..ret[i].key:nodeValue()..'",'
      qd = qd..'"user_activity":"'..ret[i].user_activity:nodeValue()..'",'
      qd = qd..'"device":"'..ret[i].device:nodeValue()..'",'
      qd = qd..'"item_name":"'..ret[i].item_name:nodeValue()..'",'
      qd = qd..'"companyname":"'..ret[i].companyname:nodeValue()..'",'
      qd = qd..'"contact_pref":"'..ret[i].contact_pref:nodeValue()..'",'
      qd = qd..'"dob":"'..ret[i].dob:nodeValue()..'",'
      qd = qd..'"gender":"'..ret[i].gender:nodeValue()..'",'
      qd = qd..'"location":"'..ret[i].location:nodeValue()..'"'
      qd = qd..'}'
      trace(qd)
      --iguana.logInfo(qd)
      queue.push{data=qd}
   end
end

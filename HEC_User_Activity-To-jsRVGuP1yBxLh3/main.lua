require('amplefi')
require('node')
-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   iguana.stopOnError(false)
   if Data ~= '' then
      local jsn = json.parse{data=Data}
      if timeblockExists(jsn.time_stamp) then
         -- update
         --UpdatePerfLog(jsn)
      else
         -- insert
         InsertUserActivity(jsn)
      end
   end
end

function timeblockExists(ts)
   local tmp = false
   local sql = "SELECT time_stamp FROM ecompass.dbo.user_activity WHERE time_stamp = '"..ts.."'"
   local rec = amplefi.ExecuteMsSql(sql)
   if #rec > 0 then
      tmp = true
   end
   --[[if iguana.isTest() then
      tmp = false
   end--]]
   return tmp
end

function InsertUserActivity(jsn)
   local usr = amplefi.DbValue(jsn.userid)
   local lname,fname = GetUserName(jsn.clientid..'_'..jsn.userid)
   local iname = getItemName(jsn.key,jsn.clientid)
   if usr ~= '' then
      usr = amplefi.GetEncryptedData(usr)
   end
   local sql = "INSERT INTO [ecompass].[dbo].[user_activity] ([time_stamp],[service],[device]"
   sql = sql..",[userid],[clientid],[key],[user_activity],[item_name],[companyname],[contact_pref]"
   sql = sql..",[dob],[gender],[location],[last_name],[first_name]) VALUES ('"..jsn.time_stamp.."','"..jsn.service.."','"
   sql = sql..jsn.device.."','"..usr.."','"..jsn.clientid.."','"..jsn.key.."','"
   sql = sql..jsn.user_activity.."','"..amplefi.DbValue(iname).."','"..jsn.companyname.."','"
   sql = sql..jsn.contact_pref.."','"..jsn.dob.."','"..jsn.gender.."','"..jsn.location.."','"
   sql = sql..lname.."','"..fname.."');"
   trace(sql)
   if not iguana.isTest() then
      local result = amplefi.ExecuteMsSql(sql)
   end
end

function getItemName(k,c)
   local ky = 'ui_highmark_'
   if c:upper() == 'HMK' then
      ky = ky..'ben_'..k
   else
      ky = ky..c..'_'..k
   end
   trace(ky)
   local x = k:split('_')
   local retval = ''
   if x[1]:upper() == 'MESSAGES' then
      ky = ky:gsub('messages','message')
   end
   local jsn = amplefi.GetKvsValue(ky)
  
   if x[1]:upper() == 'STORY' then
      if jsn.status == '200' then
         if jsn.kvs_payload.value[1].storyName ~= '' then
            retval = jsn.kvs_payload.value[1].storyName
         else
            retval = jsn.kvs_payload.value[1].storyNav
         end
      end
   elseif x[1]:upper() == 'MESSAGES' then
      if jsn.status == '200' then
         retval = jsn.kvs_payload.value[1].msgName
      end
   end
   return retval
end

function UpdatePerfLog(jsn)
   --trace(jsn.kv.storyName)
   local sql = "UPDATE ecompass.dbo.perf_log SET session_data = '"..jsn.session_data.."' "
   sql = sql.."WHERE [time_stamp] = '"..jsn.ts.."';"
   local result = amplefi.ExecuteMsSql(sql)
end

function GetUserName(ukey)
   local sql = "SELECT * FROM auth.users WHERE user_key = '"..ukey.."';"
   local rst = amplefi.ExecuteMySql(sql)
   local ln,fn = '',''
   if #rst > 0 then
      ln = amplefi.GetEncryptedData(rst[1].user_last_name:nodeValue())
      fn = amplefi.GetEncryptedData(rst[1].user_first_name:nodeValue())
   end
   return ln,fn
end
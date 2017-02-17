require 'amplefi'
-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   iguana.setTimeout(600)
   if Data ~= '' then
      local jsn = json.parse{data=Data}
      if jsn.empid ~= nil and jsn.empid ~= '' then
         local encrypt = false
         local code = jsn.clientid
         local user_key = code..'_'..jsn.empid
         local user_id = jsn.empid
         local user_email = amplefi.DbValue(jsn.workemail)
         if encrypt then
            user_key = amplefi.GetEncryptedData(user_key)
            user_id = amplefi.GetEncryptedData(jsn.empid)
            if jsn.workemail ~= '' then
               user_email = amplefi.GetEncryptedData(jsn.workemail)
            end
         end
         local user_attributes = '{"companyname":"'..jsn.companyname..'"'
         ..',"location":"'..jsn.location..'","gender":"'..jsn.gender..'"'
         ..',"dob":"'..jsn.dob..'","app":"ecompass","clientid":"'..code..'"}'
         local sql = "SELECT * FROM auth.users WHERE user_key = '"..user_key.."'"
         local result = amplefi.ExecuteMySql(sql)
          local dbvals = ''
         local csvvals = user_key
         if #result ~= 0 then
            sql = "UPDATE auth.users SET user_email = '"..user_email.."'"
            sql = sql.." WHERE user_key = '"..user_key.."'"
         else
            -- Add the user into the table
            sql = "INSERT INTO auth.users (user_key,user_id,user_email"
            ..",user_attributes,role,user_password,user_phone,user_first_name,"
            .."user_last_name,user_socket_id) VALUES ('"..user_key.."','"..user_id
            .."','"..user_email.."','"..user_attributes.."',' ',' ',' ',' ',' ',' ')"
         end
         if sql:sub(1,6):upper() ~= 'SELECT' then
            result = amplefi.ExecuteMySql(sql)
         end
      end
   end
end


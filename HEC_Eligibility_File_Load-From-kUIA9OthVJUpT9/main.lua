--local curl = require 'luacurl'
-- The main function is the first function called from Iguana.
require 'stringutil'
require 'amplefi'

local encrypt = false

function main()
   iguana.stopOnError(false)
   -- send each line in the file for processing to
   -- the outbound translator code for processing.
   processFile('/ecompass/','Highmark_Elig_Emp.csv','hmk')
   util.sleep(5000)
   processFile('/ahnauto/','AHN_Elig_Emp.csv','ahn')
   --processFile1('/ahnauto/','AHN2.csv','ahn')
end

function getInactiveKeys(d,cid)
   local sql = "TRUNCATE TABLE auth.tmpActive_Keys;"
   local result = amplefi.ExecuteMySql(sql)
   sql = "SELECT user_key FROM auth.users WHERE user_attributes LIKE '%ecompass%' "
   sql = sql.."AND user_attributes LIKE '%"..cid.."%'"
   local ck = amplefi.ExecuteMySql(sql)
   local uk = ''
   local fd = {}
   for i = 2, #d do
      fd = amplefi.ParseCSVLine(d[i],'|')
      uk = cid..'_'
      uk = uk..tostring(fd[2])
      sql = "INSERT INTO auth.tmpActive_Keys (user_key,client_code) VALUES ('"
      sql = sql..uk.."','"..cid.."')"
      --trace(sql)
      result = amplefi.ExecuteMySql(sql)
   end
   sql = "SELECT user_key FROM auth.vwInactive_Keys WHERE client_code = '"..cid.."'"
   result = amplefi.ExecuteMySql(sql)
   return result
end

function processFile(RemoteDir,RemoteFile,ClientID)
   local conn = amplefi.ConnectToExaVault()
   local flist = conn:list{remote_path=RemoteDir}
   local fexist = false
   for x = 1, #flist do
      if flist[x].filename:upper() == RemoteFile:upper() then
         fexist = true
      end
   end
   if fexist then
      -- get the contents of the file
      local contents = conn:get{remote_path=RemoteDir..RemoteFile}
      -- split the contents into individual lines
      local fdata = {}
      if ClientID == 'hmk' then
         fdata = contents:split('\r\n')
      elseif ClientID == 'ahn' then
         fdata = contents:split('\r\n')
      else
         -- Do nothing
      end
      -- get active keys
      local akeys = getActiveKeys(fdata,ClientID)
      --trace(#akeys)
      local ckeys,dkeys = getCurrentKeys(ClientID,akeys)
      if not iguana.isTest() then
         DeleteInactiveUserKeys(dkeys)
         --trace(#ckeys)
         --trace(#dkeys)
         -- column headers
         local cols = amplefi.ParseCSVLine('"clientid"|'..fdata[1],'|')
         local rdata = {}
         local pdata = ''
         for r = 2, #fdata do
            pdata = '{'
            rdata = amplefi.ParseCSVLine('"'..ClientID..'"|'..fdata[r],'|')
            for c = 1, #cols do
               if c ~= 1 then
                  pdata = pdata..','
               end
               pdata = pdata..'"'..cols[c]..'":'
               trace(rdata[c])
               if rdata[c] == nil then
                  pdata = pdata..'""'
               else
                  pdata = pdata..'"'..tostring(rdata[c])..'"'
               end
            end
            pdata = pdata..',"app":"ecompass"}'
            --trace(pdata)
            queue.push{data=pdata}
         end
         local ofp = RemoteDir..RemoteFile
         local dt = os.date('*t')
         local fdt = amplefi.DateTableToString(dt,'YYYYMMDDHHNNSS')
         -- added the archive folder for audit purposes for Marcel 08/04/2015
         local nfp = '/ecompass/archive/'..RemoteFile:sub(1,RemoteFile:len()-4)..'_'..fdt..RemoteFile:sub(-4)
         --trace(ofp)
         amplefi.BackupRemoteFile(ofp,nfp)
         amplefi.DeleteRemoteFile(ofp)
      end
   end
end

function DeleteInactiveUserKeys(dkeys)
   for i = 1, #dkeys do
      local sql = "DELETE FROM auth.users WHERE user_key = '"..dkeys[i].."'"
      --trace(sql)
      --iguana.logWarning('SQL: '..sql)
      --local result = amplefi.ExecuteMySql(sql)
   end
end

function UserKeyIsActive(ukey,akeys)
   local tmp = false
   for i = 1, #akeys do
      trace(akeys[i])
      trace(ukey)
      if akeys[i] == ukey then
         tmp = true
      end
   end
   return tmp
end

function getActiveKeys(fdata,ClientID)
   local akeys = {}
   local a = 1
   local aval = ''
   local ldata = {}
   for i = 2, #fdata do
      ldata = amplefi.ParseCSVLine(fdata[i],'|')
      aval = ClientID..'_'..tostring(ldata[2])
      if encrypt then
         aval = tostring(amplefi.GetEncryptedData(aval))
      end
      table.insert(akeys,a,aval)
      a = a + 1
   end
   return akeys
end

function getCurrentKeys(ID,akeys)
   local sql = "SELECT user_key FROM auth.users WHERE user_attributes "
   sql = sql.."LIKE '%ecompass%' AND user_attributes LIKE '%"..ID.."%'"
   local results = amplefi.ExecuteMySql(sql)
   local rkeys = {}
   local dkeys = {}
   local val = ''
   local dcnt = 0
   for i = 1, #results do
      val = tostring(results[i]):gsub("'","")
      table.insert(rkeys,i,val)
      if not UserKeyIsActive(val,akeys) then
         dcnt = dcnt + 1
         table.insert(dkeys,dcnt,val)
      end
   end
   return rkeys,dkeys
end

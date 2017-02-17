amplefi = {}

local user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:38.0) Gecko/20100101 Firefox/38.0'

function amplefi.BackupRemoteFile(FilePath,NewFilePath)
   local conn = amplefi.ConnectToExaVault()
   local data = conn:get{remote_path=FilePath}
   local result = conn:put{remote_path=NewFilePath,data=data,overwrite=true}
end

function amplefi.DeleteRemoteFile(FilePath)
   local conn = amplefi.ConnectToExaVault()
   conn:delete{remote_path=FilePath}
end

function amplefi.ConnectToExaVault()
   local sname = amplefi.sftpURL
   local uname = amplefi.sftpUname
   local pword = amplefi.sftpPword
   -- Initialize sFTP connection
   local sftpConn = net.sftp.init{server=sname, username=uname, password=pword,live=true,timeout=90000}
   return sftpConn
end

function amplefi.GetListOfFiles(Path)
   -- Initialize sFTP connection
   local conn = amplefi.ConnectToExaVault()
   local olist = conn:list{remote_path=Path}
   local flist = {}
   local j = 0
   for i = 1, #olist do
      if olist[i].is_retrievable then
         j = j + 1
         flist[j] = olist[i]
      end
   end
   return flist
end

function amplefi.FileExistsInExaVault(RemoteDir,RemoteFile,Connection)
   local olist = Connection:list{remote_path=RemoteDir}
   local tmp = false
   for i = 1, #olist do
      if olist[i].is_retrievable and
         olist[i].filename:upper() == RemoteFile:upper()then
         tmp = true
      end
   end
   return tmp
end
	
function amplefi.FileExistsInExaVaultStarting(RemoteDir,RemoteFile,CharCount,Connection)
   local olist = Connection:list{remote_path=RemoteDir}
   local tmp = ''
   for i = 1, #olist do
      if olist[i].is_retrievable and
         olist[i].filename:upper():sub(1,CharCount) == RemoteFile:upper():sub(1,CharCount) then
         tmp = RemoteDir..olist[i].filename
      end
   end
   return tmp
end

function amplefi.ParseCSVLine(line,sep)
  local res = {}
  local pos = 1
  sep = sep or ','
  while true do
     local c = string.sub(line,pos,pos)
     if (c == "") then break end
      local posn = pos
      local ctest = string.sub(line,pos,pos)
      trace(ctest)
      while ctest == ' ' do
         -- handle space(s) at the start of the line (with quoted values)
         posn = posn + 1
         ctest = string.sub(line,posn,posn)
         if ctest == '"' then
            pos = posn
            c = ctest
         end
      end
      if (c == '"') then
         -- quoted value (ignore separator within)
         local txt = ""
         repeat
            local startp,endp = string.find(line,'^%b""',pos)
            txt = txt..string.sub(line,startp+1,endp-1)
            pos = endp + 1
            c = string.sub(line,pos,pos)
            if (c == '"') then
               txt = txt..'"'
               -- check first char AFTER quoted string, if it is another
               -- quoted string without separator, then append it
               -- this is the way to "escape" the quote char in a quote. example:
               -- value1,"blub""blip""boing",value3 will result in blub"blip"boing for the middle
            elseif c == ' ' then
               -- handle space(s) before the delimiter (with quoted values)
               while c == ' ' do
                  pos = pos + 1
                  c = string.sub(line,pos,pos)
               end
            end
         until (c ~= '"')
         table.insert(res,txt)
         trace(c,pos,i)
         if not (c == sep or c == "") then
            error("ERROR: Invalid CSV field - near character "..pos.." in this line of the CSV file: \n"..line, 3)
         end
         pos = pos + 1
         posn = pos
         ctest = string.sub(line,pos,pos)
         trace(ctest)
         while ctest == ' ' do
            -- handle space(s) after the delimiter (with quoted values)
            posn = posn + 1
            ctest = string.sub(line,posn,posn)
            if ctest == '"' then
               pos = posn
               c = ctest
            end
         end
      else
         -- no quotes used, just look for the first separator
         local startp,endp = string.find(line,sep,pos)
         if (startp) then
            table.insert(res,string.sub(line,pos,startp-1))
            pos = endp + 1
         else
            -- no separator found -> use rest of string and terminate
            table.insert(res,string.sub(line,pos))
            break
         end
      end
   end
   return res
end

function amplefi.GetEncryptedData(val)
   iguana.logInfo('GetEncryptedData started.')
   --local tmp = val:gsub(' ','%%20')
   --local sURL = 'http://localhost:8150/sign?data='..tmp   --..val:gsub(' ','%20')
   local edata = ''
   if val ~= '' then
      local dt, er = net.http.get{
         url = 'https://secure.guardedata.com/sign',     
         headers={['User-Agent']=user_agent},
         live = true,
         parameters = {['data']=val},
         timeout=60000
      }
      local jsn = json.parse{data=dt}
      if jsn.status == '200' then
         edata = jsn.payload
      else
         iguana.stopOnError(true)
         iguana.logError('Error: '..jsn.status..' - '..jsn.exception)
      end
   end
   iguana.logInfo('GetEncryptedData completed.')
   return edata
end

function amplefi.GetDecryptedData(val)
   --local tmp = val:gsub(' ','%%20')
   --local sURL = 'http://localhost:8150/sign?data='..tmp   --..val:gsub(' ','%20')
   local edata = ''
   if val ~= '' then
      local dt, er = net.http.get{
         url = 'https://secure.guardedata.com/unsign',     
         headers={['User-Agent']=user_agent},
         live = true,
         parameters = {['data']=val},
         timeout=60000
      }
      local jsn = json.parse{data=dt}
      if jsn.status == '200' then
         edata = jsn.payload
      else
         iguana.stopOnError(true)
         iguana.logError('Error: '..jsn.status..' - '..jsn.exception)
      end
   end
   return edata
end

function amplefi.GetLatLngFromDb(Params)
   local lat = ''
   local lng = ''
   local sql = "SELECT lat,lng FROM ahn.vw_distinct_addresses "
   sql = sql.."WHERE CONCAT(REPLACE(addr1,'''',''''''),' ',city,' ',state,' ',zip) = '"
   sql = sql..Params.location:gsub("'","''").."' LIMIT 1"
   local result = amplefi.ExecuteMySql(sql)
   if #result > 0 then
      lat = tostring(result[1].lat:nodeValue())
      lng = tostring(result[1].lng:nodeValue())
   end
   return lat,lng
end

function amplefi.GetLatLong(Params)
   iguana.logInfo('GetLatLong started.')
   local lat,lng = amplefi.GetLatLngFromDb(Params)
   local token = ''
   local jsn = {}
   if lat == '' then
      token = amplefi.GetGeospatialToken()
      if token ~= '' then
         Params.token = token
         jsn = amplefi.GeocodeValidateLocation(Params)
         if jsn.status == '401' then
            local sql = "DELETE FROM `auth`.`iguana_key` WHERE `id`=2;" 
            local rst = amplefi.ExecuteMySql(sql)
            Params.token = amplefi.GetGeospatialToken()
            jsn = amplefi.GeocodeValidateLocation(Params)
            if jsn.status == '200' then
               lat = jsn.payload.lat
               lng = jsn.payload.lng
            else
               iguana.logError('amplefi.GetLatLong error: '..jsn.exception)
            end
         elseif jsn.status == '200' then
            lat = jsn.payload.lat
            lng = jsn.payload.lng
         else
            iguana.logError('amplefi.GetLatLong error: '..jsn.exception)
         end
      end
   end
   iguana.logInfo('GetLatLong completed.')
   return lat,lng
end

function amplefi.GeocodeValidateLocation(Params)
   Params.ver = '5.1.1'
   local dt, er = net.http.get{
      url = 'https://geospatial.amplefi.co/geocode_validate_location',     
      headers={['User-Agent']=user_agent},
      parameters = Params,
      live = true,
      timeout=60000
   }   
	local jsn = json.parse{data=dt}
   return jsn
end

function amplefi.GetKvsValue(k)
   local token = amplefi.GetToken()
   local p = {}
   local jsn = {}
   if token ~= '' then
      p.token = token
      p.key = k
      jsn = amplefi.GetKvsJson(p)
      if jsn.status == '401' then
         local sql = "DELETE FROM `auth`.`iguana_key` WHERE `id`=1;" 
         local rst = amplefi.ExecuteMySql(sql)
         p.token = amplefi.GetToken()
         jsn = amplefi.GetKvsJson(p)
      end
   end
   return jsn
end

function amplefi.GetKvsJson(Params)
   Params.ver = '5.1.0'
   Params.method = 'json'
   Params.action = 'get'
   local dt, er = net.http.get{
      url = 'https://global.amplefi.co/kvs',     
      headers={['User-Agent']=user_agent},
      parameters = Params,
      live = true,
      timeout=60000
   }  
   local jsn = json.parse{data=dt}
   return jsn
end

function amplefi.GetGeospatialToken()
   local token = ''
   local sql = 'SELECT `id`,`key` FROM `auth`.`iguana_key` WHERE `id`=2;'
   local rst = amplefi.ExecuteMySql(sql)
   if #rst > 0 then
      token = rst[1].key:nodeValue()
   else
      local p = {}
      p.ver = '5.1.1'
      p.user = 'valbanese@amplefi.com'
      p.pwd = '7xgLvqiHqpjXkeCm4lRxamqNzdDCPs'
      p.domain = 'amplefi'
      local dt,er = net.http.get{
         url = 'https://geospatial.amplefi.co/auth_authenticate',     
         headers={['User-Agent']=user_agent},
         parameters = p,
         live = true,
         timeout=60000
      }   
      local jsn = json.parse{data=dt}
      if jsn.status == '200' then
         token = jsn.payload.master_key
         sql = "INSERT INTO `auth`.`iguana_key` (`id`,`key`) VALUES (2,'"..token.."');"
         local rec = amplefi.ExecuteMySql(sql)
      end
   end
   return token
end

function amplefi.GetToken()
   local token = ''
   local sql = 'SELECT `id`,`key` FROM `auth`.`iguana_key` WHERE `id`=1;'
   local rst = amplefi.ExecuteMySql(sql)
   local dt = {}
   local er = 0
   local p = {}
   p.user = 'valbanese@amplefi.com'
   p.pwd = 'albanese'
   p.client = 'acs'
   if #rst > 0 then
      token = rst[1].key:nodeValue()
   else
      dt,er = net.http.get{
         url = 'https://global.amplefi.co/auth_authenticate',     
         headers={['User-Agent']=user_agent},
         parameters = p,
         live = true,
         timeout=60000
      }   
      local jsn = json.parse{data=dt}
      if jsn.status == '200' then
         token = jsn.payload.master_key
         sql = "INSERT INTO `auth`.`iguana_key` (`id`,`key`) VALUES (1,'"..token.."');"
         local rec = amplefi.ExecuteMySql(sql)
      end
   end
   return token
end

function amplefi.ExecuteSql(sql,conn)
   local result = conn:execute{sql=sql,live=true}
   return result
end

function amplefi.ExecuteMySql(sql)
   local conn = db.connect{api=db.MY_SQL
      ,name='cmh@corev4.cqbvdgbrltru.us-east-1.rds.amazonaws.com:3306'
      ,user='root',password='8SgFKr1OcNMyrE',use_unicode=true,live=true}
   local result = conn:execute{sql=sql,live=true}
   conn:close()
   return result
end

function amplefi.ExecuteMsSql(sql)
   local conn = db.connect{api=db.SQL_SERVER,name='mysqlserver1'
      ,user='root',password='1xcxsGQAk)hgv4g',live=true,timeout=600}
   local result = conn:execute{sql=sql,live=true}
   conn:close()
   return result
end
--[[
function amplefi.ExecuteMsSqlDb(sql,dbname)
   local conn = db.connect{api=db.SQL_SERVER,name=dbname
      ,user='root',password='1xcxsGQAk)hgv4g',live=true}
   local result = conn:execute{sql=sql,live=true}
   conn:close()
   return result
end
--]]
function amplefi.ExecuteMsSqlDb(sql,dbname)
   local conn = db.connect{api=db.SQL_SERVER,name=dbname
      ,user='root',password='1xcxsGQAk)hgv4g',live=true}
   while true do
      local success,result = pcall(conn.execute,conn,{sql=sql,live=true,timeout=900})
      if success then
         break
      else
         iguana.logInfo('amplefi.ExecuteMsSqlDb error:\n'..result)
      end
   end
   conn:close()
   return result
end

function amplefi.DbValue(val)
   local retVal = ''
   if val ~= nil and tostring(val) ~= 'NULL' then
      retVal = val:gsub("'","''")
      retVal = retVal:gsub('"','""')
      retVal = retVal:gsub("[\128-\255]", "")  -- strip unprintable characters
   end
   return retVal
end

function amplefi.MergeMySql(Data,dbname)
   db.merge{api=db.MY_SQL, name=dbname, live=true,
      user='root', password='8SgFKr1OcNMyrE', data=Data}
end

function amplefi.MergeMsSql(Data,dbname)
   db.merge{api=db.SQL_SERVER, name=dbname, live=true,
      user='root', password='1xcxsGQAk)hgv4g', data=Data}
end

function amplefi.DateTableToString(DateTable,Format)
   -- return a formatted string from a date table
   local dt = DateTable
   local fdt = ''
   if Format == 'YYYYMMDDHHNNSS' then
      fdt = dt.year..('0'..dt.month):reverse():sub(1,2):reverse()
      fdt = fdt..('0'..dt.day):reverse():sub(1,2):reverse()
      fdt = fdt..('0'..dt.hour):reverse():sub(1,2):reverse()
      fdt = fdt..('0'..dt.min):reverse():sub(1,2):reverse()
      fdt = fdt..('0'..dt.sec):reverse():sub(1,2):reverse()
   elseif Format == 'YYYY-MM-DD' then
      fdt = dt.year..'-'..('0'..dt.month):reverse():sub(1,2):reverse()
      fdt = fdt..'-'..('0'..dt.day):reverse():sub(1,2):reverse()
   end
   return fdt
end

function amplefi.GetLocalizer(params)
   local token = 'eyJ1c2VyIjoiZXZpdmFyZXNAYW1wbGVmaS5jb20ifQ.CN3bWA.g5ukZklRw4sPgZi5qTG5vOastDRFSx7ABWiu1BwyIis'
   --local token = GetToken()
   local distance = ''
   local state = ''
   params.token = token
   local dt, er = net.http.get{
      url = 'https://geospatial.amplefi.co/geocode_localizer',     
      headers={['User-Agent']=user_agent},
      parameters = params,
      live = true,
      timeout=60000
   }   --trace(dt)
   if er == 200 then
      local jsn = json.parse{data=dt}
      distance = jsn.payload.distance
      state = jsn.payload.state
   end
   return distance,state
end

function amplefi.GetMobileToken()
   local token = ''
   local siteURL = 'https://geospatial.amplefi.co/auth_mobile_authenticate?'
   siteURL = siteURL..'user=notifications@amplefi.com&pwd=o3D*GtVVhb4qCscs'
   local dt, er = net.http.get{
      url = siteURL,     
--      debug = true,
      live = true,
      timeout=60000
   }
   if er == 200 then
      local jsn = json.parse{data=dt}
      token = jsn.payload.master_key
   end
   return token
end

amplefi.sftpURL = 'iqpace.exavault.com'
amplefi.sftpUname = 'tq1'
amplefi.sftpPword = 'Buckeye54'
amplefi.mssqlServer = 'mysqlserver1'
amplefi.mssqlUname = 'root'
amplefi.mssqlPword = '1xcxsGQAk)hgv4g'
amplefi.mssqlRdsServer = 'consensus'
amplefi.mssqlRdsUname = 'python'
amplefi.mssqlRdsPword = '7lJy7FKII1Dg'

return amplefi

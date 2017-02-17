gsCrypt = {}

function gsCrypt.GetEncryptedData(val)
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

function gsCrypt.GetDecryptedData(val)
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

return gsCrypt
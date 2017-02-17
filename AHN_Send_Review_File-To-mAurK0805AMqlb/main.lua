require('amplefi')
-- The main function is the first function called from Iguana.
function main(Data)
   iguana.setTimeout(600)
   --DeleteUsers()
   --GetC2VUsers()
   if Data ~= '' then
      WriteData(Data)
   end
end

function WriteData(qd)
   local jsn = json.parse{data=qd}
   local rfn = jsn.rpath
   local lfn = jsn.lpath
   local fm = jsn.fmode
   local fd = jsn.data
   local conn = amplefi.ConnectToExaVault()
   local lf = io.open(lfn,fm)
   local status = lf:write(fd)
   status = lf:flush()
   status = lf:close()
   if jsn.position:upper() == 'END' then
      local pfile = conn:put{remote_path=rfn,local_path=lfn,overwrite=true}
   end
   iguana.logInfo('Log: AHN Send Review File '..jsn.position..'.')      
end

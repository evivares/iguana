require 'valence'

function valence.loadMedClaims(fn)
   trace(fn)
   -- Declare and initialize variables
   local npicnt = 0
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   if iguana.isTest() then
      providerfilepath = '/chs/to_amplefi/med_claims/MED_CLAIMS.txt'
   end
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Loop through each line of data
   for i = 2, #d do
      local ld = d[i]
      if ld ~= '' then
         local q = '[{"FileType":"med_claim"},{"rowpos":"'..i..'"}'
         q = q..',{"FileName":"'..fn..'"},{"payload":"'..ld..'"}]'
         trace(q)
         queue.push{data=q}
      end
   end
   q = '[{"FileType":"med_claim"},{"rowpos":"rowend"}'
   q = q..',{"FileName":"'..fn..'"},{"payload":""}]'
   queue.push{data=q}
end

function valence.loadRxClaims(fn)
   trace(fn)
   -- Declare and initialize variables
   local npicnt = 0
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   if iguana.isTest() then
      providerfilepath = '/chs/to_amplefi/rx_claims/RX_CLAIMS.txt'
   end
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Loop through each line of data
   for i = 2, #d do
      local ld = d[i]
      if ld ~= '' then
         local q = '[{"FileType":"rx_claim"},{"rowpos":"'..i..'"}'
         q = q..',{"FileName":"'..fn..'"},{"payload":"'..ld..'"}]'
         trace(q)
         queue.push{data=q}
      end
   end
   q = '[{"FileType":"rx_claim"},{"rowpos":"rowend"}'
   q = q..',{"FileName":"'..fn..'"},{"payload":""}]'
   queue.push{data=q}
end
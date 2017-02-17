require('io')
require('stringutil')
require('node')
require('amplefi')
require('datediff')

function debug(A) return end

-- The main function is the first function called from Iguana.
function main()
   if not iguana.isTest() then
      iguana.setTimeout(900)
      -- List files in remote directory
      local rdlist = amplefi.GetListOfFiles('/ahnauto/')
      local tmp = ''
      local x = tmp:upper():find('.DOCX')
      if x == nil then
         x = 0
      end
      --local gpl = 'GoldenProviderList.txt'   -- Golden Provider List filename
      for i = 1, #rdlist do
         tmp = rdlist[i].filename
         --trace(tmp:upper():find('.DOCX'))
         if tmp:upper() == 'GOLDENPROVIDERLIST.TXT' then
            ProcessDoctorList('GoldenProviderList.txt')
            ProcessRegeocoding()
            NoLatLngNotification()
         elseif tmp:upper() == 'LOCATIONLIST.TXT' then
            ProcessLocationList4('LocationList.txt')
         elseif tmp:upper() == 'LOCATIONLISTNOPCP.TXT' then
            ProcessLocationList4('LocationListNoPCP.txt')
         elseif tmp:upper() == 'GOLDENBIOLIST.TXT' then
            ProcessBioList('GoldenBioList.txt')
         elseif tmp:upper() == 'GOLDENMXLIST.TXT' then
            ProcessMxList('GoldenMXList.txt')
         elseif tmp:upper() == 'GOLDENPROVIDERREVIEW.TXT' or
            tmp:upper() == '.' or 
            tmp:upper() == '..' or
            tmp:upper() == 'THUMBS.DB' or
            tmp:upper() == 'AHN2.CSV' or
            tmp:upper() == 'AHN_ELIG_EMP.CSV' or
            x > 0 then
            -- Skip these
            trace('Skipped')
         else
            -- Write file to log for email notification later
            iguana.logInfo('Unknown AHN File Received: '..tmp)
         end
      end
   end
   -- Write memory used by this channel into the log
   --local MemUsed = collectgarbage('count')
   --iguana.logInfo('AHN to Database TEST Lua memory: ' .. MemUsed/1024 .. ' MB')
end

function NoLatLngNotification()
   local s = "SELECT * FROM ahn.vw_dr_address_without_lat_lng"
   local r = amplefi.ExecuteMySql(s)
   if #r > 0 then
      iguana.logInfo("Missing Lat/Lng Info...count = "..#r)
   end
end

function ProcessRegeocoding()
   --1.  Get date last re-geocoding was done.
   local sql = "SELECT date_regeocode FROM ahn.regeocode_log "
   sql = sql.."ORDER BY date_regeocode DESC LIMIT 1"
   local rst = amplefi.ExecuteMySql(sql)
   local r = rst[1].date_regeocode:nodeValue()..''
   local n = os.date('*t')
   local y1 = r:sub(3,4)
   local m1 = r:sub(6,7)
   local d1 = r:sub(9,10)
   local y2 = (n.year..''):sub(3,4)
   local m2 = ('0'..n.month):reverse():sub(1,2):reverse()
   local d2 = ('0'..n.day):reverse():sub(1,2):reverse()
   local diff = daysbetween(m1..'/'..d1..'/'..y1,m2..'/'..d2..'/'..y2)
   if #rst == 0 then
      diff = 31
   end
   --trace(diff)
   if diff >= 30 then
      sql = "SELECT * FROM `ahn`.`vw_address_npi_count` ORDER BY `npi_count`" 
      if iguana.isTest() then
         sql = sql.." LIMIT 3"
      end
      local rec = amplefi.ExecuteMySql(sql)
      local q = ''
      for i = 1,#rec do
         local a = rec[i].addr1:nodeValue()
         local c = rec[i].city:nodeValue()
         local s = rec[i].state:nodeValue()
         local z = rec[i].zipcode:nodeValue()
         local l = a..' '..c..' '..s..' '..z
         local p = {
            ['source']='GoogleV3',
            ['location']=l,
         }
         local lat,lng = amplefi.GetLatLong(p)
         local pos = ''
         q = '{"addr1":"'..a..'",'
         q = q..'"city":"'..c..'",'
         q = q..'"state":"'..s..'",'
         q = q..'"zip":"'..z..'",'
         q = q..'"latitude":"'..lat..'",'
         q = q..'"longitude":"'..lng..'",'
         q = q..'"listname":"regeocode",'
         if i == 1 then
            pos = 'start'
         elseif i == #rec then
            pos = 'end'
         else
            pos = 'middle'
         end
         q = q..'"position":"'..pos..'"}'
         if not iguana.isTest() then
            queue.push{data=q}
         end
      end
   end
end

function ProcessMxList(FileName)
   -- Declare and initialize variables
   local npicnt = 0
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/ahnauto/'..FileName
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local ldata = contents:split('\r\n')
   -- Get the column names.
   local flist = ldata[1]:split('|')
   -- This for loop pairs each column with each value.
   for j = 2, #ldata do
      pdata = '{'
      fdata = ldata[j]:split('|')
      trace(fdata[1])
      if fdata[1] ~= '' then
         for k = 1, #flist do
            if k ~= 1 then
               pdata = pdata..','
            end
            fd = fdata[k]
            if fd == nil then
               fd = ''
            end
            pdata = pdata..'"'..flist[k]..'":"'..fd..'"'
         end
         pdata = pdata..',"listname":"MXList","position":"'
         if j == 2 then
            pdata = pdata..'start'
         elseif j == #ldata or fdata[1]:upper() == 'WEXFORD' then
            pdata = pdata..'end'
         else
            pdata = pdata..'middle'
         end
         pdata = pdata..'"}'
         trace(pdata)
         queue.push{data=pdata}
      end
   end
   local dt = os.date('*t')
   local fdt = amplefi.DateTableToString(dt,'YYYYMMDDHHNNSS')
   -- added the archive folder for audit purposes for Marcel 08/04/2015
   local nfp = '/ahn_archive/GoldenProviderList/GoldenMXList_'..fdt..'.txt'
   if not iguana.isTest() then
      amplefi.BackupRemoteFile(providerfilepath,nfp)
      amplefi.DeleteRemoteFile(providerfilepath)
   end
end
   
function ProcessBioList(FileName)
   -- Declare and initialize variables
   local npicnt = 0
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/ahnauto/'..FileName
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local ldata = contents:split('\r\n')
   -- Get the column names.
   local flist = ldata[1]:split('|')
   -- This for loop pairs each column with each value.
   for j = 2, #ldata do
      pdata = '{'
      fdata = ldata[j]:split('|')
      for k = 1, #flist do
         if k ~= 1 then
            pdata = pdata..','
         end
         fd = fdata[k]
         if fd == nil then
            fd = ''
         end
         --if flist[k]:upper() == 'NPI' then
         --   npicnt = npicnt + 1
         --   table.insert(npis,npicnt,fdata[k])
         --end
         pdata = pdata..'"'..flist[k]..'":"'..fd..'"'
      end
      pdata = pdata..',"listname":"BioList","position":"'
      if j == 2 then
         pdata = pdata..'start'
      elseif j == #ldata then
         pdata = pdata..'end'
      else
         pdata = pdata..'middle'
      end
      pdata = pdata..'"}'
      trace(pdata)
      queue.push{data=pdata}
   end
   if not iguana.isTest() then
      --DeleteProvidersNotInList(npis)
      local dt = os.date('*t')
      local fdt = amplefi.DateTableToString(dt,'YYYYMMDDHHNNSS')
      -- added the archive folder for audit purposes for Marcel 08/04/2015
      local nfp = '/ahn_archive/GoldenProviderList/GoldenBioList_'..fdt..'.txt'
      amplefi.BackupRemoteFile(providerfilepath,nfp)
      amplefi.DeleteRemoteFile(providerfilepath)
   end
end

function ProcessDoctorList(FileName)
   -- Declare and initialize variables
   local conn = amplefi.ConnectToExaVault()
   --local locationfilepath = '/ahnauto/'..FileName
   local providerfilepath = '/ahnauto/'..FileName
   --local locationfilepath = '/elvin/ahn_services/'..FileName
   local mlps = {}  -- main level pages only
   local mlpds = {} -- main level page details
   local mlpcnt = 0
   local mlpdcnt = 0
   local npis = {}
   local npicnt = 0
   local fdata = {}   -- Field data
   local fd = ''
   local pdata = ''   -- Data reformatted for processing
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local ldata = contents:split('\r\n')
   -- Get the column names.
   local flist = ldata[1]:split('|')
   -- This for loop pairs each column with each value.
   for j = 2, #ldata do
      pdata = '{'
      fdata = ldata[j]:split('|')
      for k = 1, #flist do
         if k ~= 1 then
            pdata = pdata..','
         end
         fd = fdata[k]
         if fd == nil then
            fd = ''
         end
         if flist[k]:upper() == 'NPI' then
            npicnt = npicnt + 1
            table.insert(npis,npicnt,fdata[k])
         end
         pdata = pdata..'"'..flist[k]..'":"'..fd..'"'
      end
      pdata = pdata..',"listname":"DoctorList","position":"'
      if j == 2 then
         pdata = pdata..'start'
      elseif j == #ldata then
         pdata = pdata..'end'
      else
         pdata = pdata..'middle'
      end
      pdata = pdata..'"}'
      --trace(pdata)
      queue.push{data=pdata}
   end
   if not iguana.isTest() then
      --DeleteProvidersNotInList(npis)
      local dt = os.date('*t')
      local fdt = amplefi.DateTableToString(dt,'YYYYMMDDHHNNSS')
      -- added the archive folder for audit purposes for Marcel 08/04/2015
      local nfp = '/ahn_archive/GoldenProviderList/GoldenProviderList_'..fdt..'.txt'
      amplefi.BackupRemoteFile(providerfilepath,nfp)
      amplefi.DeleteRemoteFile(providerfilepath)
   end
end

function ProcessLocationList4(FileName)
   -- Declare and initialize variables
   local conn = amplefi.ConnectToExaVault()
   local locationfilepath = '/ahnauto/'..FileName
   --local locationfilepath = '/elvin/ahn_services/'..FileName
   local mlps = {}  -- main level pages only
   local mlpds = {} -- main level page details
   local mlpcnt = 0
   local mlpdcnt = 0
   local fdata = {}   -- Field data
   local pdata = ''   -- Data reformatted for processing
   -- Get the contents of the file.
   local contents = conn:get{remote_path=locationfilepath}
   -- Separate each row of data for processing.
   local ldata = contents:split('\r\n')
   -- Get the column names.
   local fieldlist = ldata[1]:split('|')
   -- This for loop pairs each column with each value.
   local HasDetails = false
   local mlpnds = {}
   local mlpndcnt = 0
   for j = 2, #ldata do
      HasDetails = false
      pdata = ''
      fdata = ldata[j]:split('|')
      --trace(fdata)
      local pcpb,pcpe = ldata[j]:upper():find('PRIMARY CARE PRACTICE')
      if pcpb == nil then
         for k = 1, #fdata do
            if fieldlist[k]:upper() == 'MAINLEVELPAGE' 
               and fdata[k]:upper() == 'Y' then
               mlpcnt = mlpcnt + 1
               table.insert(mlps,mlpcnt,fdata)
            elseif fieldlist[k]:upper() == 'MAINLEVELPAGE' 
               and fdata[k]:upper() ~= 'Y' then
               HasDetails = true
               mlpdcnt = mlpdcnt + 1
               table.insert(mlpds,mlpdcnt,fdata)
            end
         end
      end
      if not HasDetails then
         mlpndcnt = mlpndcnt + 1
         table.insert(mlpnds,mlpndcnt,fdata)
      end
   end
   --trace(mlpnds)
   --local x = {}
   local qcnt = 1
   local qdata = ''
   local qd = ''
   for i = 1, #mlps do
      qcnt = 1
      qdata = '['
      pdata = generateAhnServiceProvider(mlps[i])
      --trace(pdata)
      for j = 1, #mlpds do
         if mlps[i][1]:upper() == mlpds[j][1]:upper() then
            if qcnt ~= 1 then
               qdata = qdata..','
            end
            qdata = qdata..generateAhnServiceQuality(mlpds[j],qcnt)
            qcnt = qcnt + 1
         end
      end
      qdata = qdata..']'
      if qdata == '[]' then
         qdata = generateAhnServiceQualityNoDetail(mlps[i])
      end
      pdata = pdata..qdata..'],"listname":"LocationList"}'
      --trace(pdata)
      local ix1,ix2 = pdata:find('"quality":')
      --x[i] = pdata
      -- Send each provider to queue.
      --trace(pdata)
      queue.push{data=pdata}
   end
   --trace(x[115])
   local dt = os.date('*t')
   local fdt = amplefi.DateTableToString(dt,'YYYYMMDDHHNNSS')
   -- added the archive folder for audit purposes for Marcel 08/04/2015
   local nfp = '/ahn_archive/LocationList/'..FileName:gsub('.txt','')..'_'..fdt..'.txt'
   amplefi.BackupRemoteFile(locationfilepath,nfp)
   amplefi.DeleteRemoteFile(locationfilepath)
end

function ProcessLocationList3(FileName)
   -- Declare and initialize variables
   local conn = amplefi.ConnectToExaVault()
   local locationfilepath = '/ahnauto/'..FileName
   --local locationfilepath = '/elvin/ahn_services/'..FileName
   local mlps = {}  -- main level pages only
   local mlpds = {} -- main level page details
   local mlpcnt = 0
   local mlpdcnt = 0
   local fdata = {}   -- Field data
   local pdata = ''   -- Data reformatted for processing
   -- Get the contents of the file.
   local contents = conn:get{remote_path=locationfilepath}
   -- Separate each row of data for processing.
   local ldata = contents:split('\r\n')
   -- Get the column names.
   local fieldlist = ldata[1]:split('|')
   -- This for loop pairs each column with each value.
   for j = 2, #ldata do
      pdata = ''
      fdata = ldata[j]:split('|')
      --trace(fdata)
      local pcpb,pcpe = ldata[j]:upper():find('PRIMARY CARE PRACTICE')
      if pcpb == nil then
         for k = 1, #fdata do
            if fieldlist[k]:upper() == 'MAINLEVELPAGE' 
               and fdata[k]:upper() == 'Y' then
               mlpcnt = mlpcnt + 1
               table.insert(mlps,mlpcnt,fdata)
            elseif fieldlist[k]:upper() == 'MAINLEVELPAGE' 
               and fdata[k]:upper() ~= 'Y' then
               mlpdcnt = mlpdcnt + 1
               table.insert(mlpds,mlpdcnt,fdata)
            end
         end
      end
   end
   local x = {}
   local qcnt = 1
   local qdata = ''
   for i = 1, #mlps do
      qcnt = 1
      qdata = '['
      pdata = generateAhnServiceProvider(mlps[i])
      --trace(pdata)
      for j = 1, #mlpds do
         if mlps[i][1]:upper() == mlpds[j][1]:upper() then
            if qcnt ~= 1 then
               qdata = qdata..','
            end
            qdata = qdata..generateAhnServiceQuality(mlpds[j],qcnt)
            qcnt = qcnt + 1
         end
      end
      qdata = qdata..']'
      pdata = pdata..qdata..'],"listname":"LocationList"}'
      x[i] = pdata
      -- Send each provider to queue.
      --trace(pdata)
      queue.push{data=pdata}
   end
   --trace(x[115])
   local dt = os.date('*t')
   local fdt = amplefi.DateTableToString(dt,'YYYYMMDDHHNNSS')
   -- added the archive folder for audit purposes for Marcel 08/04/2015
   local nfp = '/ahn_archive/LocationList/'..FileName:gsub('.txt','')..'_'..fdt..'.txt'
   amplefi.BackupRemoteFile(locationfilepath,nfp)
   amplefi.DeleteRemoteFile(locationfilepath)
end

function ProcessLocationList(FileName)
   -- Declare and initialize variables
   local conn = amplefi.ConnectToExaVault()
   local locationfilepath = '/ahnauto/'..FileName
   --local locationfilepath = '/elvin/ahn_services/'..FileName
   local mlps = {}  -- main level pages only
   local mlpds = {} -- main level page details
   local mlpcnt = 0
   local mlpdcnt = 0
   local fdata = {}   -- Field data
   local pdata = ''   -- Data reformatted for processing
   -- Get the contents of the file.
   local contents = conn:get{remote_path=locationfilepath}
   -- Separate each row of data for processing.
   local ldata = contents:split('\r\n')
   -- Get the column names.
   local fieldlist = ldata[1]:split('|')
   -- This for loop pairs each column with each value.
   for j = 2, #ldata do
      pdata = ''
      fdata = ldata[j]:split('|')
      --trace(fdata)
      local pcpb,pcpe = ldata[j]:upper():find('PRIMARY CARE PRACTICE')
      if pcpb == nil then
         for k = 1, #fdata do
            --pdata = pdata..fieldlist[k]..'='..fdata[k]..'|'
            if fieldlist[k]:upper() == 'MAINLEVELPAGE' 
               and fdata[k]:upper() == 'Y' then
               mlpcnt = mlpcnt + 1
               table.insert(mlps,mlpcnt,fdata)
--               pdata = generateAhnServiceProvider(fdata)
            elseif fieldlist[k]:upper() == 'MAINLEVELPAGE' 
               and fdata[k]:upper() ~= 'Y' then
               mlpdcnt = mlpdcnt + 1
               table.insert(mlpds,mlpdcnt,fdata)
            end
         end
      end
   end
   local x = {}
   local qcnt = 1
   local qdata = ''
   for i = 1, #mlps do
      qcnt = 1
      qdata = '['
      pdata = generateAhnServiceProvider(mlps[i])
      --trace(pdata)
      for j = 1, #mlpds do
         if mlps[i][1]:upper() == mlpds[j][1]:upper() then
            if qcnt ~= 1 then
               qdata = qdata..','
            end
            qdata = qdata..generateAhnServiceQuality(mlpds[j],qcnt)
            qcnt = qcnt + 1
         end
      end
      qdata = qdata..']'
      pdata = pdata..qdata..'],"listname":"LocationList"}'
      x[i] = pdata
      -- Send each provider to queue.
      --trace(pdata)
      queue.push{data=pdata}
   end
   --trace(x[115])
   local dt = os.date('*t')
   local fdt = amplefi.DateTableToString(dt,'YYYYMMDDHHNNSS')
   -- added the archive folder for audit purposes for Marcel 08/04/2015
   local nfp = '/ahn_archive/LocationList/LocationList_'..fdt..'.txt'
   amplefi.BackupRemoteFile(locationfilepath,nfp)
   amplefi.DeleteRemoteFile(locationfilepath)
end

function generateAhnServiceQualityNoDetail(csv)
   --trace(csv)
   local cdata = '{"qcnt":"1"'
   cdata = cdata..',"schedule":{},"Street Address 2":"'..csv[7]..'"'
   cdata = cdata..',"County":"'..csv[11]..'"'
   cdata = cdata..',"Phone":"'..csv[12]..'"'
   cdata = cdata..',"Fax":"'..csv[13]..'"'
   cdata = cdata..',"BuildingName":"'..csv[2]..'"'
   cdata = cdata..',"FacilityType":"'..csv[5]..'"'
   cdata = cdata..',"DiagnosticServices":['..getList(csv[21])..']'
   cdata = cdata..',"OutpatientCareServices":['..getList(csv[22])..']'
   cdata = cdata..'}'
   --trace(cdata)
   return cdata
end

function generateAhnServiceQuality(csv,qcnt)
   --trace(csv)
   local cdata = '{"qcnt":"'..qcnt..'"'
   cdata = cdata..',"schedule":{'
   if csv[14] == '' then
      cdata = cdata..'"MON":"Call for an Appointment"'
   else
      cdata = cdata..'"MON":"'..csv[14]..'"'
   end
   if csv[15] == '' then
      cdata = cdata..',"TUE":"Call for an Appointment"'
   else
      cdata = cdata..',"TUE":"'..csv[15]..'"'
   end
   if csv[16] == '' then
      cdata = cdata..',"WED":"Call for an Appointment"'
   else
      cdata = cdata..',"WED":"'..csv[16]..'"'
   end
   if csv[17] == '' then
      cdata = cdata..',"THU":"Call for an Appointment"'
   else
      cdata = cdata..',"THU":"'..csv[17]..'"'
   end
   if csv[18] == '' then
      cdata = cdata..',"FRI":"Call for an Appointment"'
   else
      cdata = cdata..',"FRI":"'..csv[18]..'"'
   end
   if csv[19] == '' then
      cdata = cdata..',"SAT":"Call for an Appointment"'
   else
      cdata = cdata..',"SAT":"'..csv[19]..'"'
   end
   if csv[20] == '' then
      cdata = cdata..',"SUN":"Call for an Appointment"'
   else
      cdata = cdata..',"SUN":"'..csv[20]..'"'
   end
   cdata = cdata..'},"Street Address 2":"'..csv[7]..'"'
   cdata = cdata..',"County":"'..csv[11]..'"'
   cdata = cdata..',"Phone":"'..csv[12]..'"'
   cdata = cdata..',"Fax":"'..csv[13]..'"'
   cdata = cdata..',"BuildingName":"'..csv[2]..'"'
   cdata = cdata..',"FacilityType":"'..csv[5]..'"'
   cdata = cdata..',"DiagnosticServices":['..getList(csv[21])..']'
   cdata = cdata..',"OutpatientCareServices":['..getList(csv[22])..']'
   cdata = cdata..'}'
   --trace(cdata)
   return cdata
end

function generateAhnServiceProvider(csv)
   local cdata = '{"provider":{'
   cdata = cdata..'"name":"'..csv[1]..'"'
   cdata = cdata..',"about":"'..csv[4]..'"'
   cdata = cdata..',"addr1":"'..csv[6]..'"'
   cdata = cdata..',"city":"'..csv[8]..'"'
   cdata = cdata..',"state":"'..csv[9]..'"'
   cdata = cdata..',"zip":"'..csv[10]..'"'
   cdata = cdata..',"phone":"'..csv[12]..'"'
   cdata = cdata..',"service_type":"AHN Service"'
   cdata = cdata..',"latitude":"'..csv[23]..'"'
   cdata = cdata..',"longitude":"'..csv[24]..'"'
   cdata = cdata..',"hh_partner":"YES"'
   cdata = cdata..',"adjacent":"1"'
   cdata = cdata..'},"quality":['
   --trace(cdata)
   return cdata
end

function getList(csv)
   local lst = ''
   local tmp = {}
   if csv ~= '' then
      tmp = csv:split(',')
      for i = 1, #tmp do
         if i ~= 1 then
            lst = lst..','
         end
         lst = lst..'"'..tmp[i]..'"'
      end
   end
   return lst
end

function ProcessGoldenList(FileName)
   -- Declare and initialize variables
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/ahnauto/'..FileName
   local npis = {}
   local npicnt = 0
   local fdata = {}   -- Field data
   local pdata = ''   -- Data reformatted for processing
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local ldata = contents:split('\r')
   -- Get the column names.
   local fieldlist = ldata[1]:gsub('\n',''):split('|')
   -- This for loop pairs each column with each value.
   for j = 2, #ldata do
      pdata = ''
      fdata = ldata[j]:gsub('\n',''):split('|')
      for k = 1, #fdata do
         pdata = pdata..fieldlist[k]..'='..fdata[k]..'|'
         if fieldlist[k]:upper() == 'NPI' then
            npicnt = npicnt + 1
            table.insert(npis,npicnt,fdata[k])
         end
      end
      -- Send each line of data for processing.
      --trace(pdata)
      queue.push{data=pdata}
   end
   DeleteProvidersNotInList(npis)
   local dt = os.date('*t')
   local fdt = amplefi.DateTableToString(dt,'YYYYMMDDHHNNSS')
   -- added the archive folder for audit purposes for Marcel 08/04/2015
   local nfp = '/ahn_archive/GoldenProviderList/GoldenProviderList_'..fdt..'.txt'
   amplefi.BackupRemoteFile(providerfilepath,nfp)
   amplefi.DeleteRemoteFile(providerfilepath)
end

function DeleteProvidersNotInList(NewList)
   local sql = "SELECT DISTINCT npi FROM ahn.GoldenProviderList"
   local results = amplefi.ExecuteMySql(sql)
   local pdata = ''
   local dcnt = 0
   local sql = "DELETE FROM ahn.GoldenProviderList WHERE npi IN ("
   for i = 1, #results do
      if not InNewList(NewList,results[i].npi) then
         dcnt = dcnt + 1
         if dcnt == 1 then
            sql = sql.."'"..results[i].npi.."'"
         else
            sql = sql..",'"..results[i].npi.."'"
         end
      end
   end
   sql = sql..")"
   if sql:find('()',1,true) == nil then
      results = amplefi.ExecuteMySql(sql)
   end
end

function InNewList(NewList,npi)
   local retval = false
   for i = 1, #NewList do
      --trace(NewList[i]..' - '..npi)
      if tostring(NewList[i]) == tostring(npi) then
         retval = true
      end
   end
   return retval
end

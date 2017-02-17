require 'valence'

function valence.loadEmplanelment(fn)
   iguana.logInfo(iguana.channelName()..' : FromChannel loadEmplanelment start.')
   -- Declare and initialize variables
   local npicnt = 0
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   if iguana.isTest() then
      providerfilepath = '/chs/to_amplefi/empanelment/empanelment.txt'
   end
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Get the column names refer to documentation
   -- https://guardiansolutions.atlassian.net/wiki/pages/viewpage.action?pageId=17662024
   local hd = {}
   hd[1] = 'LineNumber'
   hd[2] = 'LinkageID'
   hd[3] = 'RecordType'
   hd[4] = 'SSN'
   hd[5] = 'RIN'
   hd[6] = 'DOB'
   hd[7] = 'LastName'
   hd[8] = 'FirstName'
   hd[9] = 'MiddleInitial'
   hd[10] = 'MedicalHomeID'
   hd[11] = 'MedicalHomeBeginDate'
   hd[12] = 'MedicalHomeEndDate'
   hd[13] = 'AppAssistorOrg'
   hd[14] = 'CareCoordinatorID'
   local hdr = d[1]:split('; ')
   local rc = hdr[1]:split(': ')
   local fl = hdr[2]:split(': ')
   local dc = hdr[3]:split(': ')
   local ac = tostring(#d - 1)
   local dt,rowpos = '',''
   --if rc == ac then
      -- This for loop pairs each column with each value.
   for j = 2, #d do
      --local ld = d[j]:split(',')
      local ld = amplefi.ParseCSVLine(d[j],',')
      dt = '[{"FileType":"empanelment"}'
      dt = dt..',{"rowpos":"'..rowpos..'"}'
      dt = dt..',{"FileName":"'..fn..'"}'
      for k = 1, #ld do
         dt = dt..',{"'..hd[k]..'":"'..ld[k]..'"}'
      end
      trace(dt)
      --local x,y = dt:find('{"LineNumber":}')
      --if x ~= nil then
         -- End of file
      --   dt = dt:gsub('{"LineNumber":}','{"LineNumber":""}')
      --   rowpos = 'rowend'
      --else
         if j == 2 then
            rowpos = 'rowstart'
         else
            rowpos = 'rowmiddle'
         end
      --end
      dt = dt..']'
      trace(dt)
      local jsn = json.parse{data=dt}
      queue.push{data=dt}
   end
   dt = '[{"FileType":"empanelment"}'
   dt = dt..',{"rowpos":"rowend"}'
   dt = dt..',{"FileName":"'..fn..'"}'
   dt = dt..']'
   queue.push{data=dt}
   iguana.logInfo(iguana.channelName()..' : FromChannel loadEmplanelment end.')
end

function valence.processEmpanelment(jsn,Data)
   iguana.logInfo(iguana.channelName()..' : ToChannel processEmpanelment start.')
   if jsn[2].rowpos ~= 'rowend' then
      trace('start processing')
      valence.insertEmpanelment(jsn,Data)
   else
      trace('delete file after processing')
      -- delete or archive the file from the sftp site.
      local fn = '/chs/to_amplefi/'..jsn[3].FileName
      if not iguana.isTest() then
         local cnnSftp = amplefi.ConnectToExaVault()
         cnnSftp:delete{remote_path=fn}
      else
         trace(fn)
      end
   end
   iguana.logInfo(iguana.channelName()..' : ToChannel processEmpanelment end.')
end

function valence.insertEmpanelment(jsn,Data)
   iguana.logInfo(iguana.channelName()..' : ToChannel insertEmpanelment start.')
   local x,y = Data:find('LineNumber')
   if x ~= nil then
      local conn = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
         ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
         ,timeout=600}

      local sql = 
      [[
      INSERT INTO cchhs.dbo.empanelment
      (
      [LineNumber],
      [LinkageID],
      [RecordType],
      [SSN],
      [RIN],
      [DOB],
      [LastName],
      [FirstName],
      [MiddleInitial],
      [MedicalHomeID],
      [BeginDate],
      [EndDate],
      [AppAssistorOrg],
      [CareCoordinatorID],
      [FileName]
      )
      VALUES
      (
      ]]..
      "'"..jsn[4].LineNumber.."',"..
      "\n   '"..jsn[5].LinkageID.."',"..
      "\n   '"..jsn[6].RecordType.."',"..
      "\n   '"..jsn[7].SSN.."',"..
      "\n   '"..jsn[8].RIN.."',"..
      "\n   '"..jsn[9].DOB.."',"..
      "\n   '"..amplefi.DbValue(jsn[10].LastName).."',"..
      "\n   '"..amplefi.DbValue(jsn[11].FirstName).."',"..
      "\n   '"..jsn[12].MiddleInitial.."',"..
      "\n   '"..amplefi.DbValue(jsn[13].MedicalHomeID).."',"..
      "\n   '"..jsn[14].MedicalHomeBeginDate.."',"..
      "\n   '"..jsn[15].MedicalHomeEndDate.."',"..
      "\n   '"..jsn[16].AppAssistorOrg.."',"..
      "\n   '"..jsn[17].CareCoordinatorID.."',"..
      "\n   '"..jsn[3].FileName.."'"..
      '\n   )'  
      if not iguana.isTest() then
         local s,r = pcall(conn.execute,{
               api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
               user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
               sql=sql,live=true})
         if not s then
            iguana.logError('Insert empanelment error:\n\n'..r..'\n\n'..sql)
         end
      else
         trace(sql)
      end
   end
   iguana.logInfo(iguana.channelName()..' : ToChannel insertEmpanelment end.')
end


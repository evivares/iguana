require 'valence'


function valence.loadPriorAuth(fn)
   iguana.logInfo(iguana.channelName()..' : FromChannel loadPriorAuth start.')
   -- Declare and initialize variables
   local rowpos = ''
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   if iguana.isTest() then
      providerfilepath = '/chs/to_amplefi/priorauth/priorauth.csv'
   end
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Get the column names.
   local hdr = amplefi.ParseCSVLine(d[1],',')
   -- This for loop pairs each column with each value.
   local q = ''
   for j = 2, #d do
      local ld = amplefi.ParseCSVLine(d[j],',')
      q = '[{"FileType":"priorauth"}'
      if j == 2 then
         q = q..',{"rowpos":"rowstart"}'
      else
         q = q..',{"rowpos":"rowmiddle"}'
      end
      q = q..',{"FileName":"'..fn..'"}'
      for k = 1, #ld do
         q = q..',{"'..hdr[k]..'":"'..ld[k]:gsub('\\','')..'"}'
      end
      q = q..']'
      --trace(q)
      local x,y = q:find('CME')
      --local jsn = json.parse{data=q}
      --trace(jsn)
      if x ~= nil then
         queue.push{data=q}
      end
   end
   q = '[{"FileType":"priorauth"}'
	q = q..',{"rowpos":"rowend"}'
   q = q..',{"FileName":"'..fn..'"}'
   for i = 1, #hdr do
      q = q..',{"'..hdr[i]..'":""}'
   end
   q = q..']'
   trace(q)
   local j = json.parse{data=q}
	queue.push{data=q}
   iguana.logInfo(iguana.channelName()..' : FromChannel loadPriorAuth end.')
end

function valence.processPriorAuth(jsn,Data)
   iguana.logInfo(iguana.channelName()..' : ToChannel loadPriorAuth start.')
   iguana.stopOnError(false)
   if jsn[2].rowpos ~= 'rowend' then
      local x,y = Data:find('Member ID')
      if x ~= nil then
         trace('start processing')
         valence.insertPriorAuth(jsn,Data)
      end
   else
      -- delete or archive the file from the sftp site.
      local fn = '/chs/to_amplefi/'..jsn[3].FileName
      if not iguana.isTest() then
         local cnnSftp = amplefi.ConnectToExaVault()
         cnnSftp:delete{remote_path=fn}
      else
         trace(fn)
      end
   end
   iguana.logInfo(iguana.channelName()..' : ToChannel loadPriorAuth end.')
end

function valence.insertPriorAuth(jsn,Data)
   iguana.logInfo(iguana.channelName()..' : ToChannel insertPriorAuth start.')
   local conn = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
      ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
      ,timeout=600}
   
   local sql = 
   [[
   INSERT INTO cchhs.dbo.authorizations
   (
   [CME]
   ,[Member ID]
   ,[Member Last Name]
   ,[Member First Name]
   ,[Member DOB]
   ,[Service Date Admit Date]
   ,[Auth Expiration Date   Discharge Date]
   ,[Approved   Denied]
   ,[Authorization #]
   ,[Authorization Type]
   ,[Authorization Category]
   ,[Procedure Code]
   ,[Approved Procedure Units]
   ,[Diag Code 1]
   ,[Diag Code 1 Type]
   ,[Diag Code 2]
   ,[Diag Code 2 Type]
   ,[Diag Code 3]
   ,[Diag Code 3 Type]
   ,[Level of Care]
   ,[Provider Name]
   ,[Facility Provider Name]
   ,[Requesting Provider Name]
   ,[Current Decision Date]
   ,[Priority]
   ,[FileName]
   )
   VALUES
   (
   ]]..
   "'"..jsn[4].CME.."',"..
   "\n   '"..jsn[5]["Member ID"].."',"..
   "\n   '"..amplefi.DbValue(jsn[6]["Member Last Name"]).."',"..
   "\n   '"..amplefi.DbValue(jsn[7]["Member First Name"]).."',"..
   "\n   '"..jsn[8]["Member DOB"].."',"..
   "\n   '"..jsn[9]["Service Date/Admit Date"].."',"..
   "\n   '"..jsn[10]["Auth Expiration Date / Discharge Date"].."',"..
   "\n   '"..jsn[11]["Approved / Denied"].."',"..
   "\n   '"..jsn[12]["Authorization #"].."',"..
   "\n   '"..amplefi.DbValue(jsn[13]["Authorization Type"]).."',"..
   "\n   '"..jsn[14]["Authorization Category"].."',"..
   "\n   '"..jsn[15]["Procedure Code"].."',"..
   "\n   '"..jsn[16]["Approved Procedure Units"].."',"..
   "\n   '"..jsn[17]["Diag Code 1"].."',"..
   "\n   '"..jsn[18]["Diag Code 1 Type"].."',"..
   "\n   '"..jsn[19]["Diag Code 2"].."',"..
   "\n   '"..jsn[20]["Diag Code 2 Type"].."',"..
   "\n   '"..jsn[21]["Diag Code 3"].."',"..
   "\n   '"..jsn[22]["Diag Code 3 Type"].."',"..
   "\n   '"..jsn[23]["Level of Care"].."',"..
   "\n   '"..amplefi.DbValue(jsn[24]["Provider Name"]).."',"..
   "\n   '"..amplefi.DbValue(jsn[25]["Facility Provider Name"]).."',"..
   "\n   '"..amplefi.DbValue(jsn[26]["Requesting Provider Name"]).."',"..
   "\n   '"..jsn[27]["Current Decision Date"].."',"..
   "\n   '"..jsn[28].Priority.."',"..
   "\n   '"..jsn[3].FileName.."'"..
   '\n   )'  
   if not iguana.isTest() then
      local s,r = pcall(conn.execute,{
            api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
            user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
            sql=sql,live=true})
      if not s then
         iguana.logError('Insert prior authorization error:\n\n'..r..'\n\n'..sql)
      end
   else
      trace(sql)
   end
   iguana.logInfo(iguana.channelName()..' : ToChannel insertPriorAuth end.')
end


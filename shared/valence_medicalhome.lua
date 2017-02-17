require 'valence'

function valence.loadMedicalHome(fn)
   iguana.logInfo(iguana.channelName()..': FromChannel main start.')
   trace(fn)
   -- Declare and initialize variables
   local npicnt = 0
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   if iguana.isTest() then
      providerfilepath = '/chs/to_amplefi/medical_home/medical_home.txt'
   end
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Loop through each line of data
   for i = 2, #d do
      local ld = d[i]
      ld = ld:gsub('","','|')
      ld = ld:gsub('"','')
      if ld ~= '' then
         local q = '[{"FileType":"medicalhome"},{"rowpos":"'..i..'"}'
         q = q..',{"FileName":"'..fn..'"},{"payload":"'..ld..'"}]'
         trace(q)
         queue.push{data=q}
      end
   end
   q = '[{"FileType":"medicalhome"},{"rowpos":"rowend"}'
   q = q..',{"FileName":"'..fn..'"},{"payload":""}]'
   trace(q)
   queue.push{data=q}
   iguana.logInfo(iguana.channelName()..': FromChannel main end.')
end

function valence.processMedicalHome(jsn,Data)
   iguana.logInfo(iguana.channelName()..': ToChannel processMedicalHome start.')
   local fn = jsn[3].FileName
   if jsn[2].rowpos ~= 'rowend' then
      local d = jsn[4].payload:split('|')
      local sql = "SELECT * FROM cchhs.dbo.medical_home "
      sql = sql.."WHERE MedicalHomeID = '"..d[2].."' "
      sql = sql..";"
      local conn = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
         ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
         ,timeout=600}
      local s,r = pcall(conn.execute,{
            api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
            user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
            sql=sql,live=true})

            if s and #r == 0 then
         -- Insert new provider
         local c,v = '',''
         local sql1 = "INSERT INTO cchhs.dbo.medical_home ("
         for i = 1, #valence.medicalhome_hdr do
            if i > 1 then
               c = c..","
               v = v..","
            end
            c = c..valence.provider_hdr[i]
            v = v.."'"..amplefi.DbValue(d[i]).."'"
         end
         sql1 = sql1..c..") VALUES ("..v..");"
         if not iguana.isTest() then
            local conn1 = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
               ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
               ,timeout=600}
            local s1,r1 = pcall(conn1.execute,{
                  api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
                  user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
                  sql=sql1,live=true})
         else
            trace(sql1)
         end
      elseif s and #r > 0 then
         -- assuming that there is always something that has changed
         local sql2 = "UPDATE cchhs.dbo.medical_home SET "
         local cnt = 0
         
         for j = 1, #valence.medicalhome_hdr do
            if valence.medicalhome_hdr[j] ~= 'MedicalHomeID' then
               cnt = cnt + 1
               if cnt > 1 then
                  sql2 = sql2..","
               end
               sql2 = sql2..valence.medicalhome_hdr[j].." = '"..amplefi.DbValue(d[j]).."'"
            end
         end
         sql2 = sql2.." WHERE MedicalHomeID = '"..d[2].."'"
         sql2 = sql2..";"
         if cnt ~= 0 then
            if not iguana.isTest() then
               local conn2 = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
                  ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
                  ,timeout=600}
               local s2,r2 = pcall(conn2.execute,{
                     api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
                     user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
                     sql=sql2,live=true})
            else
               trace(sql2)
            end
         end
      end
   else
      -- delete or archive the file from the sftp site.
      if not iguana.isTest() then
         local cnnSftp = amplefi.ConnectToExaVault()
         fn = '/chs/to_amplefi/'..fn
         cnnSftp:delete{remote_path=fn}
      else
         trace(fn)
      end
   end
   iguana.logInfo(iguana.channelName()..': ToChannel processMedicalHome end.')
end
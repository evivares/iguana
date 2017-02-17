require 'valence'

function valence.loadCcCmWaiver(fn)
   iguana.logInfo(iguana.channelName()..' : FromChannel loadCcCmWaiver start.')
   -- Declare and initialize variables
   local rowpos = ''
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   if iguana.isTest() then
      providerfilepath = '/chs/to_amplefi/cccmwaiver/cccmwaiver.txt'
   end
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Get the column names.
   local hdr = d[1]:split('|')
   -- This for loop pairs each column with each value.
   for j = 2, #d do
      local ld = d[j]:split('|')
      local q = '[{"FileType":"cccmwaiver"}'
      if j == 2 then
         q = q..',{"rowpos":"rowstart"}'
      else
         q = q..',{"rowpos":"rowmiddle"}'
      end
      q = q..',{"FileName":"'..fn..'"}'
      for k = 1, #ld do
         q = q..',{"'..hdr[k]..'":"'..ld[k]..'"}'
      end
      q = q..']'
      trace(q)
      queue.push{data=q}
      --local jsn = json.parse{data=q}
      --trace(jsn)
   end
   q = '[{"FileType":"cccmwaiver"},{"rowpos":"rowend"},{"FileName":"'..fn..'"}'
   q = q..',{"MEMBER_ID":""},{"MONTHID":""},{"WAIVERID":""}'
   q = q..',{"START_DATE":""},{"END_DATE":""}]'
   trace(q)
	queue.push{data=q}
   iguana.logInfo(iguana.channelName()..' : FromChannel loadCcCmWaiver end.')
end

function valence.loadWaiverPull(fn)
   trace(fn)
   -- Declare and initialize variables
   local npicnt = 0
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Get the column names.
   local hdr = d[1]:split('|')
   -- This for loop pairs each column with each value.
   for j = 2, #d do
      local ld = d[j]:split('|')
   end
end

function valence.processCcCmWaiver(jsn,Data)
   iguana.logInfo(iguana.channelName()..' : ToChannel processCcCmWaiver start.')
   local rpos = jsn[2].rowpos
   local fn = jsn[3].FileName
   local memid = jsn[4].MEMBER_ID
   local monid,wid,sdate,edate = '','','',''
   if memid ~= '' then
      monid = jsn[5].MONTHID
      wid = jsn[6].WAIVERID
      sdate = jsn[7].START_DATE
      edate = jsn[8].END_DATE
   end
   if rpos ~= 'rowend' then
      if memid ~= '' then
         local sql = "SELECT * FROM cchhs.dbo.waivercccm "
         sql = sql.."WHERE MEMBER_ID = '"..memid.."' "
         sql = sql.."AND MONTHID = '"..monid.."' "
         sql = sql.."AND WAIVERID = '"..wid.."' "
         sql = sql.."AND START_DATE = '"..sdate.."';"
         local conn = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
            ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
            ,timeout=600}
         local s,r = pcall(conn.execute,{
               api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
               user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
               sql=sql,live=true})
         if s then
            -- Successful execution of query
            if #r > 0 then
               if r[1].END_DATE:nodeValue() == edate then
                  -- Just skip the record since nothing has changed.
                  trace('Nothing has changed. Just leave the record alone.')
                  sql = ''
               else
                  -- Update the END_DATE field
                  --trace('update '..r[1].END_DATE:nodeValue()..' with '..jsn[5].END_DATE)
                  sql = "UPDATE cchhs.dbo.waivercccm SET END_DATE = '"..edate.."' "
                  sql = sql.."WHERE MEMBER_ID = '"..memid.."' "
                  sql = sql.."AND MONTHID = '"..monid.."' "
                  sql = sql.."AND WAIVERID = '"..wid.."' "
                  sql = sql.."AND START_DATE = '"..sdate.."';"
               end
            else
               -- Insert jsn data to the table
               trace('insert')
               sql = "INSERT INTO cchhs.dbo.waivercccm (MEMBER_ID,MONTHID,WAIVERID,START_DATE"
               sql = sql..",END_DATE) VALUES ('"..memid.."','"..monid.."'"
               sql = sql..",'"..wid.."','"..sdate.."','"..edate.."');"
            end
            if sql ~= '' then
               local s1,r1 = pcall(conn.execute,{
                     api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
                     user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
                     sql=sql,live=true})
               if not s1 then
                  iguana.logError(r..'\nSQL: '..sql)
               end
            else
               trace('Skipping record')
            end
         else
            -- Error on query execution
            trace('error')
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
   iguana.logInfo(iguana.channelName()..' : ToChannel processCcCmWaiver end.')
end



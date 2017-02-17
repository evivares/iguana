require 'valence'

function valence.getEligibilityColumns(jsn)
   local sql = "SELECT TOP 1 * FROM cchhs.dbo.eligibility;"
   local conn = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
      ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
      ,timeout=600}
   local s,r = pcall(conn.execute,{
         api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
         user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
         sql=sql,live=true})
   local c = {}
   local cc,cv = '',''
   if s then
      -- Successful execution of query
      local c = {}
      local cc,cv = '',''
      for i = 1, #r[1] do
         c[i] = r[1][i]:nodeName()
         if i > 1 and i <= #r[1] then
            if i > 2 then
               cc = cc.."+'|'+"
               cv = cv..'|'
            end
            cc = cc..c[i]
            cv = cv..jsn[i+3][c[i]]
         end
      end
   end
   return c,cc,cv,s,r
end

function valence.eligibilityChanged(cc,cv,memid,j)
   local retval = false
   local sql = "SELECT "..cc.." AS cv,* FROM cchhs.dbo.eligibility "
   sql = sql.."WHERE MemberID = '"..memid.."';"
   local conn = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
      ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
      ,timeout=600}
   local s,r = pcall(conn.execute,{
         api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
         user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
         sql=sql,live=true})
   if s and #r > 0 then
      retval = (
         r[1].cv:nodeValue() ~= cv and 
         r[1].LastName:nodeValue():upper() == j[5].LastName:upper() and
         r[1].FirstName:nodeValue():upper() == j[6].FirstName:upper() and
         r[1].DOB:nodeValue() == j[7].DOB
      )
   end
   return retval,#r,r
end

function valence.loadEligibility(fn)
   trace(fn)
   if not iguana.isTest() then
      -- set timeout setting1
      iguana.setTimeout(3600)
   end
   -- Declare and initialize variables
   local rowpos = ''
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   if iguana.isTest() then
      providerfilepath = '/chs/to_amplefi/eligibility/eligibility.txt'
   end
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Loop through each line of data
   for i = 1, #d do
      local ld = d[i]
      if ld ~= '' then
         local q = '[{"FileType":"eligibility"},{"rowpos":"'..i..'"}'
         q = q..',{"FileName":"'..fn..'"},{"payload":"'..ld..'"}]'
         trace(q)
         queue.push{data=q}
      end
   end
   q = '[{"FileType":"eligibility"},{"rowpos":"rowend"}'
   q = q..',{"FileName":"'..fn..'"},{"payload":""}]'
   queue.push{data=q}
end

function valence.processEligibility(jsn,Data)
   iguana.logInfo('valence to mos: to channel processEligibility start.')
   local fn = jsn[3].FileName
   if jsn[2].rowpos ~= 'rowend' then
      local d = jsn[4].payload:split('|')
      local sql = "SELECT * FROM cchhs.ccc.vw_iguana_eligibility_check "
      sql = sql.."WHERE MemberID = '"..d[1].."' "
      sql = sql..";"
      local conn = db.connect{api=db.SQL_SERVER,name=amplefi.mssqlRdsServer
         ,user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,live=true
         ,timeout=600}
      local s,r = pcall(conn.execute,{
            api=db.SQL_SERVER,name=amplefi.mssqlRdsServer,
            user=amplefi.mssqlRdsUname,password=amplefi.mssqlRdsPword,
            sql=sql,live=true})
      if s and #r == 0 then
         -- Insert new elibility
         local c,v = '',''
         local sql1 = "INSERT INTO cchhs.dbo.eligibility ("
         for i = 1, #valence.elig_hdr do
            if i > 1 then
               c = c..","
               v = v..","
            end
            c = c..valence.elig_hdr[i]
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
         -- check if something has changed
         local sql2 = "UPDATE cchhs.dbo.eligibility SET "
         local cnt = 0
         local o = r[1].rec:nodeValue():split('|')
         for j = 1, #valence.elig_hdr do
            if d[j] ~= o[j] then
               cnt = cnt + 1
               if cnt > 1 then
                  sql2 = sql2..","
               end
               sql2 = sql2..valence.elig_hdr[j].." = '"..amplefi.DbValue(d[j]).."'"
            end
         end
         sql2 = sql2.." WHERE MemberID = '"..d[1].."';"
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
   iguana.logInfo('valence to mos: to channel processEligibility end.')
end

--[[
function valence.loadEligibility(fn)
   trace(fn)
   if not iguana.isTest() then
      -- set timeout setting1
      iguana.setTimeout(3600)
   end
   -- Declare and initialize variables
   local rowpos = ''
   local conn = amplefi.ConnectToExaVault()
   local providerfilepath = '/chs/to_amplefi/'..fn
   if iguana.isTest() then
      providerfilepath = '/chs/to_amplefi/eligibility/eligibility.txt'
   end
   -- Get the contents of the file.
   local contents = conn:get{remote_path=providerfilepath}
   -- Separate each row of data for processing.
   local d = contents:split('\r\n')
   -- Get the column names.
   local hdr = {}
   hdr[1] = 'MemberID'
   hdr[2] = 'LastName'
   hdr[3] = 'FirstName'
   hdr[4] = 'DOB'
   hdr[5] = 'MedicaidID'
   hdr[6] = 'RelationshipCode'
   hdr[7] = 'SSN'
   hdr[8] = 'PatientSex'
   hdr[9] = 'Address1'
   hdr[10] = 'Address2'
   hdr[11] = 'City'
   hdr[12] = 'State'
   hdr[13] = 'Zip'
   hdr[14] = 'Phone'
   hdr[15] = 'Email'
   hdr[16] = 'PcpID'
   hdr[17] = 'PcpName'
   hdr[18] = 'MemberGroup1'
   hdr[19] = 'MemberGroup2'
   hdr[20] = 'MemberGroup3'
   hdr[21] = 'MemberGroup4'
   hdr[22] = 'MemberGroup5'
   hdr[23] = 'AddMemberAttrib1'
   hdr[24] = 'AddMemberAttrib2'
   hdr[25] = 'AddMemberAttrib3'
   hdr[26] = 'AddMemberAttrib4'
   hdr[27] = 'AddMemberAttrib5'
   hdr[28] = 'EffectiveDate'
   hdr[29] = 'TerminationDate'
   hdr[30] = 'LOB'
   hdr[31] = 'BenefitPlanID'
   hdr[32] = 'EmployerGroupID'
   hdr[33] = 'County'
   hdr[34] = 'Language'
   hdr[35] = 'GuardianName'
   hdr[36] = 'CopayLevel'
   hdr[37] = 'Race'
   hdr[38] = 'MedicalEligibility'
   hdr[39] = 'RxEligibility'
   hdr[40] = 'Rider1'
   hdr[41] = 'Rider2'
   hdr[42] = 'Rider3'
   hdr[43] = 'Rider4'
   hdr[44] = 'Rider5'
   hdr[45] = 'DateOfDeath'

   -- This for loop pairs each column with each value.
   for j = 1, #d do
      local ld = d[j]:split('|')
      local q = '[{"FileType":"eligibility"}'
      if j == 1 then
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
      local jsn = json.parse{data=q}
      queue.push{data=q}
      --trace(jsn)
   end
   q = '[{"FileType":"eligibility"}'
	q = q..',{"rowpos":"rowend"}'
   q = q..',{"FileName":"'..fn..'"}'
   for i = 1, #hdr do
      q = q..',{"'..hdr[i]..'":""}'
   end
   q = q..']'
   trace(q)
   local j = json.parse{data=q}
	queue.push{data=q}
end
--]]

require 'valence'
require 'valence_provider'
require 'valence_eligibility'
require 'valence_riskscores'
require 'valence_waiver'
require 'valence_empanelment'
require 'valence_priorauth'
require 'valence_medicalhome'

-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   iguana.logInfo('valence to mos: to channel main start.')
   iguana.stopOnError(false)
   if Data ~= '' then
      local jsn = json.parse{data=Data}
      --jsn[1].FileType = 'cccmwaiver'
      if jsn[1].FileType == 'cccmwaiver' then
         valence.processCcCmWaiver(jsn,Data)
      elseif jsn[1].FileType == 'empanelment' then
         valence.processEmpanelment(jsn,Data)
      elseif jsn[1].FileType == 'priorauth' then
         valence.processPriorAuth(jsn,Data)
      elseif jsn[1].FileType == 'riskscores' then
         valence.processRiskScores(jsn,Data)
      elseif jsn[1].FileType == 'eligibility' then
         valence.processEligibility(jsn,Data)
      elseif jsn[1].FileType == 'med_claim' then
         processMedClaim(jsn,Data)
      elseif jsn[1].FileType == 'rx_claim' then
         processRxClaim(jsn,Data)
      elseif jsn[1].FileType == 'provider' then
         valence.processProvider(jsn,Data)
      elseif jsn[1].FileType == 'medicalhome' then
         valence.processMedicalHome(jsn,Data)
      end
   end
   iguana.logInfo('valence to mos: to channel main end.')
end

function processRxClaim(jsn,Data)
   iguana.logInfo('valence to mos: to channel processRxClaim start.')
   local fn = jsn[3].FileName
   if jsn[2].rowpos ~= 'rowend' then
      local d = jsn[4].payload:split('|')
      local sql = "SELECT * FROM cchhs.ccc.vw_iguana_rx_claim_check "
      sql = sql.."WHERE rec = '"..jsn[4].payload.."' "
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
         local sql1 = "INSERT INTO cchhs.dbo.rx_claim ("
         for i = 1, #valence.rxclaim_hdr do
            if i > 1 then
               c = c..","
               v = v..","
            end
            c = c..valence.rxclaim_hdr[i]
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
   iguana.logInfo('valence to mos: to channel processRxClaim end.')
end

function processMedClaim(jsn,Data)
   iguana.logInfo('valence to mos: to channel processMedClaim start.')
   local fn = jsn[3].FileName
   if jsn[2].rowpos ~= 'rowend' then
      local d = jsn[4].payload:split('|')
      local sql = "SELECT * FROM cchhs.ccc.vw_iguana_med_claim_check "
      sql = sql.."WHERE rec = '"..jsn[4].payload.."' "
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
         local sql1 = "INSERT INTO cchhs.dbo.med_claim ("
         for i = 1, #valence.medclaim_hdr do
            if i > 1 then
               c = c..","
               v = v..","
            end
            c = c..valence.medclaim_hdr[i]
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
   iguana.logInfo('valence to mos: to channel processMedClaim end.')
end

--[[
[LineNumber]
      ,[LinkageID]
      ,[RecordType]
      ,[SSN]
      ,[RIN]
      ,[DOB]
      ,[LastName]
      ,[FirstName]
      ,[MiddleInitial]
      ,[MedicalHomeID]
      ,[BeginDate]
      ,[EndDate]
      ,[AppAssistorOrg]
      ,[CareCoordinatorID]
      ,[FileName]
  FROM [cchhs].[dbo].[empanelment]
--]]

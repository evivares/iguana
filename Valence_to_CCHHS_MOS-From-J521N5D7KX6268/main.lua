require 'valence_eligibility'
require 'valence_riskscores'
require 'valence_priorauth'
require 'valence_empanelment'
require 'valence_waiver'
require 'valence_claims'
require 'valence_provider'
require 'valence_medicalhome'

-- The main function is the first function called from Iguana.
function main()
   iguana.logInfo('valence to mos: from channel main start.')
   iguana.stopOnError(false)
   iguana.setTimeout(3600)
   -- List files in remote directory
   local rpath = '/chs/to_amplefi/'
   local conn = net.sftp.init{
      server=amplefi.sftpURL,username=amplefi.sftpUname, 
      password=amplefi.sftpPword,live=true,timeout=90000}
   local flist = conn:list{remote_path=rpath}
   for i = 1, #flist do
      if flist[i].is_retrievable then
         fn = flist[i].filename
         if fn:sub(1,20) == 'Valence_CCC_PCPLink_' then
            valence.loadEmplanelment(fn)
            --      elseif fn:sub(1,13) == 'DHSS.VVRH1G99' then
            --         valence.loadWaiverPull(fn)
         elseif fn:sub(1,21) == 'COUNTYCARE_CM_WAIVER_' then
            trace('CM Waiver')
            valence.loadCcCmWaiver(fn)
         elseif fn:sub(1,15) == 'COUNTYCARE_MED_' then
            --valence.loadMedClaims(fn)
         elseif fn:sub(1,14) == 'COUNTYCARE_RX_' then
            --valence.loadRxClaims(fn)
         elseif fn:sub(1,30) == 'CountyCare_CCC_Authorizations_' then
            trace('Prior Auth')
            valence.loadPriorAuth(fn)
         elseif fn:sub(1,23) == 'COUNTYCARE_RISK_SCORES_' then
            trace('Risk Scores')
            valence.loadRiskScores(fn)
         elseif fn:sub(1,30) == 'COUNTYCARE_ELIGIBILITY_ROLLED_' then
            trace('Eligibility')
            valence.loadEligibility(fn)
         elseif fn:sub(1,20) == 'COUNTYCARE_PROVIDER_' then
            trace('Provider')
            valence.loadProvider(fn)
         elseif fn:sub(1,24) == 'Valence_CCC_MedicalHome_' then
            trace('Medical Home')
            valence.loadMedicalHome(fn)
         else
            --iguana.logInfo('Unknown file')
         end
      end
   end
   iguana.logInfo('valence to mos: from channel main end.')
end

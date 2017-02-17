require 'gsmos'

function gsmos.processVisit(PV1,guid,mcid)
   -- (1) connect to the database
   if not Conn or not Conn:check() then
      Conn = db.connect{
         api=db.SQL_SERVER,
         name=amplefi.mssqlRdsServer,
         user=amplefi.mssqlRdsUname,
         password=amplefi.mssqlRdsPword,
         live=true
      }
   end
   
   -- (2) create insert query string
   local SqlInsert =
   [[
   INSERT INTO cchhs.dbo.visit
   (
   GUID,
   MsgID,
   PatientClass,
   AdmitType,
   HospitalService,
   CurrentPointOfCare,
   CurrentRoom,
   CurrentBed,
   CurrentFacility,
   CurrentLocationType,
   AdmitSource,
   PatientType,
   FinancialClass,
   DischargeDisposition,
   DischargedToLocation,
   AdmitDateTime,
   DischargeDateTime,
   ServicingFacility,
   AccountStatus,
   VisitNumber
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   '"..PV1[2].."',"..
   "\n   '"..PV1[4].."',"..
   "\n   '"..PV1[10].."',"..
   "\n   '"..PV1[3][1].."',"..
   "\n   '"..PV1[3][2].."',"..
   "\n   '"..PV1[3][3].."',"..
   "\n   '"..PV1[3][4][1].."',"..
   "\n   '"..PV1[3][6].."',"..
   "\n   '"..PV1[14].."',"..
   "\n   '"..PV1[18].."',"..
   "\n   '"..PV1[20][1][1].."',"..
   "\n   '"..PV1[36].."',"..
   "\n   '"..PV1[37][1].."',"..
   "\n   '"..PV1[44][1].."',"..
   "\n   '"..PV1[45][1][1].."',"..
   "\n   '"..PV1[39].."',"..
   "\n   '"..PV1[41].."',"..
   "\n   '"..PV1[19][1].."'"..
   '\n   )'  
   
   -- (3) Insert data into database
   if not iguana.isTest() then
      Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
   end
end


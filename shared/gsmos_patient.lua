require 'gsmos'

function gsmos.processPatient(PID,guid,mcid)
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
   INSERT INTO cchhs.dbo.patient
   (
   GUID,
   MsgID,
   PatientID,
   LastName,
   FirstName,
   MaidenName,
   DOB,
   SSN,
   AccountNumber,
   MRN,
   Gender,
   Race,
   MaritalStatus,
   Religion,
   Language,
   Ethnicity,
   Citizenship,
   VeteranMilitaryStatus
   )
   VALUES
   (
   ]]..
   "'"..guid.."',"..
   "\n   '"..mcid.."',"..
   "\n   NULL,"..
   "\n   '"..PID[5][1][1][1].."',"..
   "\n   '"..PID[5][1][2].."',"..
   "\n   '"..PID[6][1][1][1].."',"..
   "\n   '"..PID[7][1].."',"..
   "\n   '"..PID[19].."',"..
   "\n   '"..PID[18][1].."',"..
   "\n   '"..PID[2][1].."',"..
   "\n   '"..PID[8].."',"..
   "\n   '"..PID[10][1][1].."',"..
   "\n   '"..PID[16][1].."',"..
   "\n   '"..PID[17][1].."',"..
   "\n   '"..PID[15][1].."',"..
   "\n   '"..PID[22][1][1].."',"..
   "\n   '"..PID[26][1][1].."',"..
   "\n   '"..PID[27][1].."'"..
   '\n   )'  
   
   -- (3) Insert data into database
   if not iguana.isTest() then
      -- Conn:execute{sql=SqlInsert, live=true}
   else
      trace(SqlInsert)
	end
   
end

function gsmos.getPatient(ln,fn,dob)
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
   local SqlQuery =
   [[
   SELECT * FROM cchhs.dbo.patient
   WHERE
   ]]..
   "\n   LastName = '"..ln.."'"..
   "\n   AND FirstName = '"..fn.."'"..
   "\n   AND DOB = '"..dob.."'"..
   '\n   ;'  
   
   trace(SqlQuery)
   -- (3) Query data in database
   local rs = Conn:execute{sql=SqlQuery, live=true}
   local retval = ''
   if #rs > 0 then
      retval = rs[1].GUID:nodeValue()
   end
   return retval
end

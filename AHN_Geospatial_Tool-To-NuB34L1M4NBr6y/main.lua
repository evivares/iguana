-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
require 'stringutil'
require 'amplefi'

-- List of address fields to be checked for changes.
local AddressFields = {
   'street1','city1','state1','zip1',
   'street2','city2','state2','zip2',
   'street3','city3','state3','zip3',
   'street4','city4','state4','zip4',
   'street5','city5','state5','zip5',
   'street6','city6','state6','zip6'
   }
--local token = ''

function main(Data)
   iguana.stopOnError(false)
   if not iguana.isTest() then
      iguana.setTimeout(600)
   end
   if Data ~= '' then  
      --local tmp = Data:sub(1,6):upper()
      local sql = ''
      local jsn = json.parse{data=Data}
      
      if jsn.listname:upper() == 'DOCTORLIST' or jsn.listname:upper() == 'BIOLIST' then
         if jsn.position:upper() == 'START' then
            if not iguana.isTest() then
               sql = "CREATE TABLE IF NOT EXISTS ahn.tmp"
               sql = sql..jsn.listname
               sql = sql.." (npi varchar(20) CHARACTER "
               sql = sql.."SET utf8mb4 DEFAULT NULL)"
               local ret = amplefi.ExecuteMySql(sql)
            end
         end
         if not iguana.isTest() then
            sql = "INSERT INTO ahn.tmp"..jsn.listname
            sql = sql.." (npi) VALUES ('"..jsn.npi.."');"
            local rec = amplefi.ExecuteMySql(sql)
         end
         if jsn.listname:upper() == 'DOCTORLIST' then
            ProcessDoctorList(jsn)
         else
            ProcessBioList(jsn)
         end
          --util.sleep(2000)
         if jsn.position:upper() == 'END' then
            if not iguana.isTest() then
               if jsn.listname:upper() == 'DOCTORLIST' then
                  sql = "DELETE FROM ahn.GoldenProviderList "
               else
                  sql = "DELETE FROM ahn.GoldenBioList "
               end   
               sql = sql.."WHERE npi NOT IN (SELECT DISTINCT npi FROM "
               sql = sql.."ahn.tmp"..jsn.listname..");"
               local ret = amplefi.ExecuteMySql(sql)
               sql = "DROP TABLE ahn.tmp"..jsn.listname..";"
               local rst = amplefi.ExecuteMySql(sql)
            end
         end
      elseif jsn.listname:upper() == 'LOCATIONLIST' then
         ProcessLocationList(Data)
      elseif jsn.listname:upper() == 'MXLIST' then
         ProcessMxList(jsn)
      elseif jsn.listname:upper() == 'REGEOCODE' then
         ProcessRegeocode(Data)
      end
   end
end

function ProcessRegeocode(Data)
   local jsn = json.parse{data=Data}
   RefreshRegeocodeLog(jsn.addr1,jsn.city,jsn.state,jsn.zip,jsn.latitude,jsn.longitude)
   if jsn.position:upper() == 'END' then
      local sql = "SELECT sqlcode FROM ahn.vw_Regeocode_Update_SqlCode"
      local rec = amplefi.ExecuteMySql(sql)
      if #rec > 0 then
         for i = 1, #rec do
            sql = rec[i].sqlcode:nodeValue()
            trace(sql)
            if not iguana.isTest() then
               local result = amplefi.ExecuteMySql(sql)
            end
         end
      end
   end
end

function RefreshRegeocodeLog(addr1,city,state,zip,lat,lng)
   local sql = "SELECT * FROM ahn.regeocode_log WHERE addr1='"
   sql = sql..amplefi.DbValue(addr1).."' AND city='"..city.."' "
   sql = sql.."AND state='"..state.."' AND zip='"..zip.."'"
   local rec = amplefi.ExecuteMySql(sql)
   if #rec > 0 then
      UpdateRegeocodeLog(addr1,city,state,zip,lat,lng)
   else
      AddRegeocodeLog(addr1,city,state,zip,lat,lng)
   end
end

function UpdateRegeocodeLog(a,c,s,z,lt,lg)
   local dt = os.date('%Y-%m-%d %X')
   local sql = "UPDATE ahn.regeocode_log SET latitude='"..lt.."',"
   sql = sql.."longitude='"..lg.."',date_regeocode='"..dt.."' "
   sql = sql.."WHERE addr1='"..amplefi.DbValue(a).."' AND "
   sql = sql.."city='"..c.."' AND state='"..s.."' AND zip='"..z.."';"
   --trace(sql)
   if not iguana.isTest() then
      local result = amplefi.ExecuteMySql(sql)
   end
end

function AddRegeocodeLog(a,c,s,z,lt,lg)
   local dt = os.date('%Y-%m-%d %X')
   local sql = "INSERT INTO ahn.regeocode_log (addr1,city,"
   sql = sql.."state,zip,latitude,longitude,date_regeocode) "
   sql = sql.."VALUES ('"..amplefi.DbValue(a).."','"..c.."','"
   sql = sql..s.."','"..z.."','"..lt.."','"..lg.."','"
   sql = sql..dt.."');"
   --trace(sql)
   if not iguana.isTest() then
      local result = amplefi.ExecuteMySql(sql)
   end
end

function ProcessMxList(data)
   local temp,dbdata = MxProviderExists(data.medexpresslocation)
   if temp then
      UpdateMxProviderData(data,dbdata)
   else
      AddNewMxProviderData(data)
   end
end

function UpdateMxProviderData(data,dbdata)
   local sql = "UPDATE ahn.service_providers SET provider_name = '"
   sql = sql..data.medexpresslocation.."'"
   if data.address1:upper() ~= dbdata[1].address:nodeValue():upper() then
      sql = sql..",address = '"..data.address1.."'"
   end
   if data.city:upper() ~= dbdata[1].city:nodeValue():upper() then
      sql = sql..",city = '"..data.city.."'"
   end
   if data.state:upper() ~= dbdata[1].state:nodeValue():upper() then
      sql = sql..",state = '"..data.state.."'"
   end
   if data.zip:upper() ~= dbdata[1].zip:nodeValue():upper() then
      sql = sql..",zip = '"..data.zip.."'"
   end
   if data.phone:upper() ~= dbdata[1].phone:nodeValue():upper() then
      sql = sql..",phone = '"..data.phone.."'"
   end
   local location = data.address1..' '..data.city..' '..data.state..' '..data.zip
   local params = {
      ['source']='GoogleV3',
      ['location']=location,
   }
   local lat,lng = amplefi.GetLatLong(params)
   sql = sql..",latitude = '"..lat.."'"
   sql = sql..",longitude = '"..lng.."'"
   sql = sql..",service_type = 'Med Express'"
   sql = sql..",fivestar_rating = ''"
   sql = sql..",fivestar_rating = '1900-01-01'"
   sql = sql.." WHERE cmsno = '"..dbdata[1].cmsno:nodeValue().."';"
   trace(sql)
   if not iguana.isTest() then
      local res = amplefi.ExecuteMySql(sql)
   end
   local tmp,dbd = MxQualityExists(dbdata[1].cmsno:nodeValue())
   if tmp then
      sql = "UPDATE ahn.service_quality SET answer = '"
      sql = sql.."[{\"qcnt\":\"1\",\"schedule\":{},\"Street Address 2\":\"\""
      sql = sql..",\"County\":\"\",\"Phone\":\"\",\"Fax\":\""..data.fax.."\","
      sql = sql.."\"BuildingName\":\"\",\"FacilityType\":\"\",\"DiagnosticServices\""
      sql = sql..":[],\"OutpatientCareServices\":[]}]' WHERE cmsno = '"
      sql = sql..dbd[1].cmsno:nodeValue().."';"
   else
      sql = "INSERT INTO ahn.service_quality (criteria,criteria_type,cmsno,answer) "
      sql = sql.."VALUES ('Display','service','"..dbdata[1].cmsno:nodeValue().."','"
      sql = sql.."[{\"qcnt\":\"1\",\"schedule\":{},\"Street Address 2\":\"\""
      sql = sql..",\"County\":\"\",\"Phone\":\"\",\"Fax\":\""..data.fax.."\","
      sql = sql.."\"BuildingName\":\"\",\"FacilityType\":\"\",\"DiagnosticServices\""
      sql = sql..":[],\"OutpatientCareServices\":[]}]');"
   end
   trace(sql)
   if not iguana.isTest() then
      local ret = amplefi.ExecuteMySql(sql)
   end
end

function AddNewMxProviderData(data)
   local newcmsno = GetNextAmpCmsNo()
   trace(newcmsno)
   local location = data.address1..' '..data.city..' '..data.state..' '..data.zip
   local params = {
      ['source']='GoogleV3',
      ['location']=location,
   }
   local lat,lng = '',''
   if not iguana.isTest() then
      lat,lng = amplefi.GetLatLong(params)
   end
   local sql = "INSERT INTO ahn.service_providers (cmsno,provider_name,"
   sql = sql.."address,city,state,zip,phone,latitude,longitude,localizer,"
   sql = sql.."service_type,fivestar_rating,fivestar_rating_date) "
   sql = sql.."VALUES ('"..newcmsno.."','"..amplefi.DbValue(data.medexpresslocation)
   sql = sql.."','"..amplefi.DbValue(data.address1).."','"..data.city.."','"
   sql = sql..data.state.."','"..data.zip.."','"..data.phone.."','"..lat.."','"
   sql = sql..lng.."','0','Med Express','','1900-01-01');"
   trace(sql)
   if not iguana.isTest() then
      local res = amplefi.ExecuteMySql(sql)
   end
   sql = "INSERT INTO ahn.service_quality (criteria,criteria_type,cmsno,answer) "
   sql = sql.."VALUES ('Display','service','"..newcmsno.."','"
   sql = sql.."[{\"qcnt\":\"1\",\"schedule\":{},\"Street Address 2\":\"\""
   sql = sql..",\"County\":\"\",\"Phone\":\"\",\"Fax\":\""..data.fax.."\","
   sql = sql.."\"BuildingName\":\"\",\"FacilityType\":\"\",\"DiagnosticServices\""
   sql = sql..":[],\"OutpatientCareServices\":[]}]');"
   trace(sql)
   if not iguana.isTest() then
      local ret = amplefi.ExecuteMySql(sql)
   end
end

function ProcessBioList(data)
   if data.npi ~= '' then
      local tmp,dbdata = BioExists(data.npi)
      if tmp then
         UpdateBioData(data,dbdata)
      else
         AddNewBioData(data)
      end
   end
end

function UpdateBioData(data,dbdata)
   local sql = "UPDATE ahn.GoldenBioList SET "
   local cvp = data.cvpresent:upper()
   if data.cvpresent == '' then
      cvp = 'FALSE'
   end
   sql = sql.."cvpresent = '"..cvp.."',"
   sql = sql.."bio = '"..amplefi.DbValue(data.bio).."' "
   sql = sql.."WHERE npi = '"..data.npi.."';"
   local res = amplefi.ExecuteMySql(sql)
end

function AddNewBioData(data)
   local cvp = data.cvpresent:upper()
   if data.cvpresent == '' then
      cvp = 'FALSE'
   end
   local sql = "INSERT INTO ahn.GoldenBioList (npi,cvpresent,bio) "
   sql = sql.."VALUES ('"..data.npi.."','"..cvp.."','"
   sql = sql..amplefi.DbValue(data.bio).."');"
   local res = amplefi.ExecuteMySql(sql)
end

function ProcessDoctorList(data)
   if data.npi ~= '' then
      local tmp,dbdata = DoctorExists(data.npi)
      if tmp then
         UpdateDoctorData(data,dbdata)
      else
         AddNewDoctorData(data)
      end
   end
end

function AddNewDoctorData(data)
--   if data.street1 ~= '' then
   local location = ''
   local lat1,lat2,lat3,lat4,lat5,lat6 = '','','','','',''
   local lng1,lng2,lng3,lng4,lng5,lng6 = '','','','','',''
   local params = {}
   if data.street1 ~= '' then
      location = data.street1..' '..data.city1..' '..data.state1..' '..data.zip1
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat1,lng1 = amplefi.GetLatLong(params)
   else
      iguana.logError('Error: Blank Street1 for NPI = '..data.npi)
      lat1,lng1 = '',''
   end
   if data.street2 ~= '' then
      location = data.street2..' '..data.city2..' '..data.state2..' '..data.zip2
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat2,lng2 = amplefi.GetLatLong(params)
   else
      lat2,lng2 = '',''
   end
   if data.street3 ~= '' then
      location = data.street3..' '..data.city3..' '..data.state3..' '..data.zip3
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat3,lng3 = amplefi.GetLatLong(params)
   else
      lat3,lng3 = '',''
   end
   if data.street4 ~= '' then
      location = data.street4..' '..data.city4..' '..data.state4..' '..data.zip4
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat4,lng4 = amplefi.GetLatLong(params)
   else
      lat4,lng4 = '',''
   end
   if data.street5 ~= '' then
      location = data.street5..' '..data.city5..' '..data.state5..' '..data.zip5
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat5,lng5 = amplefi.GetLatLong(params)
   else
      lat5,lng5 = '',''
   end
   if data.street6 ~= '' then
      location = data.street6..' '..data.city6..' '..data.state6..' '..data.zip6
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat6,lng6 = amplefi.GetLatLong(params)
   else
      lat6,lng6 = '',''
   end
   local sql = "INSERT INTO ahn.GoldenProviderList ("
   sql = sql.."pracid,"
   sql = sql.."title,"
   sql = sql.."firstname,"
   sql = sql.."middlename,"
   sql = sql.."lastname,"
   sql = sql.."suffix,"
   sql = sql.."profsuffix,"
   sql = sql.."npi,"
   sql = sql.."gender,"
   sql = sql.."providertype,"
   sql = sql.."hilevelspec,"
   sql = sql.."cellphone,"
   sql = sql.."boardcertified,"
   sql = sql.."primaryspecialty,"
   sql = sql.."primaryboard,"
   sql = sql.."secondaryspecialty,"
   sql = sql.."secondaryboard,"
   sql = sql.."tertiaryspecialty,"
   sql = sql.."tertiaryboard,"
   sql = sql.."otherspecialty,"
   sql = sql.."otherboard,"
   sql = sql.."emailaddress,"
   sql = sql.."groupname1,"
   sql = sql.."facility1,"
   sql = sql.."street1,"
   sql = sql.."suite1,"
   sql = sql.."location1,"
   sql = sql.."city1,"
   sql = sql.."state1,"
   sql = sql.."zip1,"
   sql = sql.."phone1,"
   sql = sql.."fax1,"
   sql = sql.."protocol1,"
   sql = sql.."groupname2,"
   sql = sql.."facility2,"
   sql = sql.."street2,"
   sql = sql.."suite2,"
   sql = sql.."location2,"
   sql = sql.."city2,"
   sql = sql.."state2,"
   sql = sql.."zip2,"
   sql = sql.."phone2,"
   sql = sql.."fax2,"
   sql = sql.."protocol2,"
   sql = sql.."groupname3,"
   sql = sql.."facility3,"
   sql = sql.."street3,"
   sql = sql.."suite3,"
   sql = sql.."location3,"
   sql = sql.."city3,"
   sql = sql.."state3,"
   sql = sql.."zip3,"
   sql = sql.."phone3,"
   sql = sql.."fax3,"
   sql = sql.."protocol3,"
   sql = sql.."groupname4,"
   sql = sql.."facility4,"
   sql = sql.."street4,"
   sql = sql.."suite4,"
   sql = sql.."location4,"
   sql = sql.."city4,"
   sql = sql.."state4,"
   sql = sql.."zip4,"
   sql = sql.."phone4,"
   sql = sql.."fax4,"
   sql = sql.."protocol4,"
   sql = sql.."groupname5,"
   sql = sql.."facility5,"
   sql = sql.."street5,"
   sql = sql.."suite5,"
   sql = sql.."location5,"
   sql = sql.."city5,"
   sql = sql.."state5,"
   sql = sql.."zip5,"
   sql = sql.."phone5,"
   sql = sql.."fax5,"
   sql = sql.."protocol5,"
   sql = sql.."groupname6,"
   sql = sql.."facility6,"
   sql = sql.."street6,"
   sql = sql.."suite6,"
   sql = sql.."location6,"
   sql = sql.."city6,"
   sql = sql.."state6,"
   sql = sql.."zip6,"
   sql = sql.."phone6,"
   sql = sql.."fax6,"
   sql = sql.."protocol6,"
   sql = sql.."school1,"
   sql = sql.."school2,"
   sql = sql.."school3,"
   sql = sql.."residency1,"
   sql = sql.."residency2,"
   sql = sql.."residency3,"
   sql = sql.."residency4,"
   sql = sql.."residency5,"
   sql = sql.."residency6,"
   sql = sql.."residency7,"
   sql = sql.."residency8,"
   sql = sql.."residency9,"
   sql = sql.."fellowship1,"
   sql = sql.."fellowship2,"
   sql = sql.."fellowship3,"
   sql = sql.."fellowship4,"
   sql = sql.."fellowship5,"
   sql = sql.."fellowship6,"
   sql = sql.."fellowship7,"
   sql = sql.."fellowship8,"
   sql = sql.."fellowship9,"
   sql = sql.."templetitle,"
   sql = sql.."templearea,"
   sql = sql.."templeinstitution,"
   sql = sql.."drexeltitle,"
   sql = sql.."drexelarea,"
   sql = sql.."drexelinstitution,"
   sql = sql.."title1,"
   sql = sql.."title2,"
   sql = sql.."title3,"
   sql = sql.."parentorg,"
   sql = sql.."employed,"
   sql = sql.."agh_7,"
   sql = sql.."avh_5,"
   sql = sql.."cgh_6,"
   sql = sql.."frh_9,"
   sql = sql.."jeff_11,"
   sql = sql.."stv_8,"
   sql = sql.."wph_10,"
   sql = sql.."clinicalexpertise1,"
   sql = sql.."clinicalexpertise2,"
   sql = sql.."clinicalexpertise3,"
   sql = sql.."clinicalexpertise4,"
   sql = sql.."clinicalexpertise5,"
   sql = sql.."clinicalexpertise6,"
   sql = sql.."clinicalexpertise7,"
   sql = sql.."clinicalexpertise8,"
   sql = sql.."clinicalexpertise9,"
   sql = sql.."clinicalexpertise10,"
   sql = sql.."clinicalexpertise11,"
   sql = sql.."clinicalexpertise12,"
   sql = sql.."clinicalexpertise13,"
   sql = sql.."clinicalexpertise14,"
   sql = sql.."clinicalexpertise15,"
   sql = sql.."clinicalexpertise16,"
   sql = sql.."clinicalexpertise17,"
   sql = sql.."clinicalexpertise18,"
   sql = sql.."clinicalexpertise19,"
   sql = sql.."clinicalexpertise20,"
   sql = sql.."clinicalexpertise21,"
   sql = sql.."clinicalexpertise22,"
   sql = sql.."clinicalexpertise23,"
   sql = sql.."clinicalexpertise24,"
   sql = sql.."clinicalexpertise25,"
   sql = sql.."ntwkalign,"
   if lat1 ~= '' then
      sql = sql.."latitude1,"
      sql = sql.."longitude1,"
   end
   if lat2 ~= '' then
      sql = sql.."latitude2,"
      sql = sql.."longitude2,"
   end
   if lat3 ~= '' then
      sql = sql.."latitude3,"
      sql = sql.."longitude3,"
   end
   if lat4 ~= '' then
      sql = sql.."latitude4,"
      sql = sql.."longitude4,"
   end
   if lat5 ~= '' then
      sql = sql.."latitude5,"
      sql = sql.."longitude5,"
   end
   if lat6 ~= '' then
      sql = sql.."latitude6,"
      sql = sql.."longitude6,"
   end
   sql = sql.."keywords1,"
   sql = sql.."dermoncall,"
   sql = sql.."zocdoc,"
   sql = sql.."epicosenabled,"
   sql = sql.."epicvisittype"
   sql = sql..") VALUES ("
   sql = sql.."'"..data.pracid.."',"
   sql = sql.."'"..amplefi.DbValue(data.title).."',"
   sql = sql.."'"..amplefi.DbValue(data.firstname).."',"
   sql = sql.."'"..amplefi.DbValue(data.middlename).."',"
   sql = sql.."'"..amplefi.DbValue(data.lastname).."',"
   sql = sql.."'"..amplefi.DbValue(data.suffix).."',"
   sql = sql.."'"..amplefi.DbValue(data.profsuffix).."',"
   sql = sql.."'"..data.npi.."',"
   sql = sql.."'"..data.gender.."',"
   sql = sql.."'"..amplefi.DbValue(data.providertype).."',"
   sql = sql.."'"..amplefi.DbValue(data.hilevelspec).."',"
   sql = sql.."'"..data.cellphone.."',"
   sql = sql.."'"..amplefi.DbValue(data.boardcertified).."',"
   sql = sql.."'"..amplefi.DbValue(data.primaryspecialty).."',"
   sql = sql.."'"..amplefi.DbValue(data.primaryboard).."',"
   sql = sql.."'"..amplefi.DbValue(data.secondaryspecialty).."',"
   sql = sql.."'"..amplefi.DbValue(data.secondaryboard).."',"
   sql = sql.."'"..amplefi.DbValue(data.tertiaryspecialty).."',"
   sql = sql.."'"..amplefi.DbValue(data.tertiaryboard).."',"
   sql = sql.."'"..amplefi.DbValue(data.otherspecialty).."',"
   sql = sql.."'"..amplefi.DbValue(data.otherboard).."',"
   sql = sql.."'"..data.emailaddress.."',"
   sql = sql.."'"..amplefi.DbValue(data.groupname1).."',"
   sql = sql.."'"..amplefi.DbValue(data.facility1).."',"
   sql = sql.."'"..amplefi.DbValue(data.street1).."',"
   sql = sql.."'"..amplefi.DbValue(data.suite1).."',"
   sql = sql.."'"..amplefi.DbValue(data.location1).."',"
   sql = sql.."'"..data.city1.."',"
   sql = sql.."'"..data.state1.."',"
   sql = sql.."'"..data.zip1.."',"
   sql = sql.."'"..data.phone1.."',"
   sql = sql.."'"..data.fax1.."',"
   sql = sql.."'"..data.protocol1.."',"
   sql = sql.."'"..amplefi.DbValue(data.groupname2).."',"
   sql = sql.."'"..amplefi.DbValue(data.facility2).."',"
   sql = sql.."'"..amplefi.DbValue(data.street2).."',"
   sql = sql.."'"..amplefi.DbValue(data.suite2).."',"
   sql = sql.."'"..amplefi.DbValue(data.location2).."',"
   sql = sql.."'"..data.city2.."',"
   sql = sql.."'"..data.state2.."',"
   sql = sql.."'"..data.zip2.."',"
   sql = sql.."'"..data.phone2.."',"
   sql = sql.."'"..data.fax2.."',"
   sql = sql.."'"..data.protocol2.."',"
   sql = sql.."'"..amplefi.DbValue(data.groupname3).."',"
   sql = sql.."'"..amplefi.DbValue(data.facility3).."',"
   sql = sql.."'"..amplefi.DbValue(data.street3).."',"
   sql = sql.."'"..amplefi.DbValue(data.suite3).."',"
   sql = sql.."'"..amplefi.DbValue(data.location3).."',"
   sql = sql.."'"..data.city3.."',"
   sql = sql.."'"..data.state3.."',"
   sql = sql.."'"..data.zip3.."',"
   sql = sql.."'"..data.phone3.."',"
   sql = sql.."'"..data.fax3.."',"
   sql = sql.."'"..data.protocol3.."',"
   sql = sql.."'"..amplefi.DbValue(data.groupname4).."',"
   sql = sql.."'"..amplefi.DbValue(data.facility4).."',"
   sql = sql.."'"..amplefi.DbValue(data.street4).."',"
   sql = sql.."'"..amplefi.DbValue(data.suite4).."',"
   sql = sql.."'"..amplefi.DbValue(data.location4).."',"
   sql = sql.."'"..data.city4.."',"
   sql = sql.."'"..data.state4.."',"
   sql = sql.."'"..data.zip4.."',"
   sql = sql.."'"..data.phone4.."',"
   sql = sql.."'"..data.fax4.."',"
   sql = sql.."'"..data.protocol4.."',"
   sql = sql.."'"..amplefi.DbValue(data.groupname5).."',"
   sql = sql.."'"..amplefi.DbValue(data.facility5).."',"
   sql = sql.."'"..amplefi.DbValue(data.street5).."',"
   sql = sql.."'"..amplefi.DbValue(data.suite5).."',"
   sql = sql.."'"..amplefi.DbValue(data.location5).."',"
   sql = sql.."'"..data.city5.."',"
   sql = sql.."'"..data.state5.."',"
   sql = sql.."'"..data.zip5.."',"
   sql = sql.."'"..data.phone5.."',"
   sql = sql.."'"..data.fax5.."',"
   sql = sql.."'"..data.protocol5.."',"
   sql = sql.."'"..amplefi.DbValue(data.groupname6).."',"
   sql = sql.."'"..amplefi.DbValue(data.facility6).."',"
   sql = sql.."'"..amplefi.DbValue(data.street6).."',"
   sql = sql.."'"..amplefi.DbValue(data.suite6).."',"
   sql = sql.."'"..amplefi.DbValue(data.location6).."',"
   sql = sql.."'"..data.city6.."',"
   sql = sql.."'"..data.state6.."',"
   sql = sql.."'"..data.zip6.."',"
   sql = sql.."'"..data.phone6.."',"
   sql = sql.."'"..data.fax6.."',"
   sql = sql.."'"..data.protocol6.."',"
   sql = sql.."'"..amplefi.DbValue(data.school1).."',"
   sql = sql.."'"..amplefi.DbValue(data.school2).."',"
   sql = sql.."'"..amplefi.DbValue(data.school3).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency1).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency2).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency3).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency4).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency5).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency6).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency7).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency8).."',"
   sql = sql.."'"..amplefi.DbValue(data.residency9).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship1).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship2).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship3).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship4).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship5).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship6).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship7).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship8).."',"
   sql = sql.."'"..amplefi.DbValue(data.fellowship9).."',"
   sql = sql.."'"..amplefi.DbValue(data.templetitle).."',"
   sql = sql.."'"..amplefi.DbValue(data.templearea).."',"
   sql = sql.."'"..amplefi.DbValue(data.templeinstitution).."',"
   sql = sql.."'"..amplefi.DbValue(data.drexeltitle).."',"
   sql = sql.."'"..amplefi.DbValue(data.drexelarea).."',"
   sql = sql.."'"..amplefi.DbValue(data.drexelinstitution).."',"
   sql = sql.."'"..amplefi.DbValue(data.title1).."',"
   sql = sql.."'"..amplefi.DbValue(data.title2).."',"
   sql = sql.."'"..amplefi.DbValue(data.title3).."',"
   sql = sql.."'"..amplefi.DbValue(data.parentorg).."',"
   sql = sql.."'"..data.employed.."',"
   sql = sql.."'"..amplefi.DbValue(data.agh_7).."',"
   sql = sql.."'"..amplefi.DbValue(data.avh_5).."',"
   sql = sql.."'"..amplefi.DbValue(data.cgh_6).."',"
   sql = sql.."'"..amplefi.DbValue(data.frh_9).."',"
   sql = sql.."'"..amplefi.DbValue(data.jeff_11).."',"
   sql = sql.."'"..amplefi.DbValue(data.stv_8).."',"
   sql = sql.."'"..amplefi.DbValue(data.wph_10).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise1).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise2).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise3).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise4).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise5).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise6).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise7).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise8).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise9).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise10).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise11).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise12).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise13).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise14).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise15).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise16).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise17).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise18).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise19).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise20).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise21).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise22).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise23).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise24).."',"
   sql = sql.."'"..amplefi.DbValue(data.clinicalexpertise25).."',"
   sql = sql.."'"..amplefi.DbValue(data.ntwkalign).."',"
   if lat1 ~= '' then
      sql = sql.."'"..lat1.."',"
      sql = sql.."'"..lng1.."',"
   end
   if lat2 ~= '' then
      sql = sql.."'"..lat2.."',"
      sql = sql.."'"..lng2.."',"
   end
   if lat3 ~= '' then
      sql = sql.."'"..lat3.."',"
      sql = sql.."'"..lng3.."',"
   end
   if lat4 ~= '' then
      sql = sql.."'"..lat4.."',"
      sql = sql.."'"..lng4.."',"
   end
   if lat5 ~= '' then
      sql = sql.."'"..lat5.."',"
      sql = sql.."'"..lng5.."',"
   end
   if lat6 ~= '' then
      sql = sql.."'"..lat6.."',"
      sql = sql.."'"..lng6.."',"
   end
   sql = sql.."'"..amplefi.DbValue(data.keywords1).."',"
   if data.dermoncall == '' then
      sql = sql.."'False',"
   else
      sql = sql.."'"..amplefi.DbValue(data.dermoncall).."',"
   end
   if data.zocdoc == '' then
      sql = sql.."'False',"
   else
      sql = sql.."'"..amplefi.DbValue(data.zocdoc).."',"
   end
   if data.epicosenabled == '' then
      sql = sql.."'False',"
   else
      sql = sql.."'"..amplefi.DbValue(data.epicosenabled).."',"
   end
   sql = sql.."'"..amplefi.DbValue(data.epicvisittype).."'"
      sql = sql..")"
   trace(sql)
   if not iguana.isTest() then
      local result = amplefi.ExecuteMySql(sql)
   end
   --sql = sql:gsub("ahn.Golden","ahn.ref.Golden")
   --result = amplefi.ExecuteMsSql(sql)
--   end
end

function UpdateDoctorData(data,dbdata)
   local sql = "UPDATE ahn.GoldenProviderList SET "
   sql = sql.."pracid = '"..data.pracid.."',"
   sql = sql.."title = '"..data.title.."',"
   sql = sql.."firstname = '"..amplefi.DbValue(data.firstname).."',"
   sql = sql.."middlename = '"..amplefi.DbValue(data.middlename).."',"
   sql = sql.."lastname = '"..amplefi.DbValue(data.lastname).."',"
   sql = sql.."suffix = '"..amplefi.DbValue(data.suffix).."',"
   sql = sql.."profsuffix = '"..amplefi.DbValue(data.profsuffix).."',"
   sql = sql.."gender = '"..data.gender.."',"
   sql = sql.."providertype = '"..amplefi.DbValue(data.providertype).."',"
   sql = sql.."hilevelspec = '"..data.hilevelspec.."',"
   sql = sql.."cellphone = '"..data.cellphone.."',"
   sql = sql.."boardcertified = '"..amplefi.DbValue(data.boardcertified).."',"
   sql = sql.."primaryspecialty = '"..amplefi.DbValue(data.primaryspecialty).."',"
   sql = sql.."primaryboard = '"..amplefi.DbValue(data.primaryboard).."',"
   sql = sql.."secondaryspecialty = '"..amplefi.DbValue(data.secondaryspecialty).."',"
   sql = sql.."secondaryboard = '"..amplefi.DbValue(data.secondaryboard).."',"
   sql = sql.."tertiaryspecialty = '"..amplefi.DbValue(data.tertiaryspecialty).."',"
   sql = sql.."tertiaryboard = '"..amplefi.DbValue(data.tertiaryboard).."',"
   sql = sql.."otherspecialty = '"..amplefi.DbValue(data.otherspecialty).."',"
   sql = sql.."otherboard = '"..amplefi.DbValue(data.otherboard).."',"
   sql = sql.."emailaddress = '"..data.emailaddress.."',"
   sql = sql.."groupname1 = '"..amplefi.DbValue(data.groupname1).."',"
   sql = sql.."facility1 = '"..amplefi.DbValue(data.facility1).."',"
   sql = sql.."street1 = '"..amplefi.DbValue(data.street1).."',"
   sql = sql.."suite1 = '"..amplefi.DbValue(data.suite1).."',"
   sql = sql.."location1 = '"..amplefi.DbValue(data.location1).."',"
   sql = sql.."city1 = '"..data.city1.."',"
   sql = sql.."state1 = '"..data.state1.."',"
   sql = sql.."zip1 = '"..data.zip1.."',"
   sql = sql.."phone1 = '"..data.phone1.."',"
   sql = sql.."fax1 = '"..data.fax1.."',"
   sql = sql.."protocol1 = '"..data.protocol1.."',"
   sql = sql.."groupname2 = '"..amplefi.DbValue(data.groupname2).."',"
   sql = sql.."facility2 = '"..amplefi.DbValue(data.facility2).."',"
   sql = sql.."street2 = '"..amplefi.DbValue(data.street2).."',"
   sql = sql.."suite2 = '"..amplefi.DbValue(data.suite2).."',"
   sql = sql.."location2 = '"..data.location2.."',"
   sql = sql.."city2 = '"..data.city2.."',"
   sql = sql.."state2 = '"..data.state2.."',"
   sql = sql.."zip2 = '"..data.zip2.."',"
   sql = sql.."phone2 = '"..data.phone2.."',"
   sql = sql.."fax2 = '"..data.fax2.."',"
   sql = sql.."protocol2 = '"..data.protocol2.."',"
   sql = sql.."groupname3 = '"..amplefi.DbValue(data.groupname3).."',"
   sql = sql.."facility3 = '"..amplefi.DbValue(data.facility3).."',"
   sql = sql.."street3 = '"..amplefi.DbValue(data.street3).."',"
   sql = sql.."suite3 = '"..amplefi.DbValue(data.suite3).."',"
   sql = sql.."location3 = '"..amplefi.DbValue(data.location3).."',"
   sql = sql.."city3 = '"..data.city3.."',"
   sql = sql.."state3 = '"..data.state3.."',"
   sql = sql.."zip3 = '"..data.zip3.."',"
   sql = sql.."phone3 = '"..data.phone3.."',"
   sql = sql.."fax3 = '"..data.fax3.."',"
   sql = sql.."protocol3 = '"..data.protocol3.."',"
   sql = sql.."groupname4 = '"..amplefi.DbValue(data.groupname4).."',"
   sql = sql.."facility4 = '"..amplefi.DbValue(data.facility4).."',"
   sql = sql.."street4 = '"..amplefi.DbValue(data.street4).."',"
   sql = sql.."suite4 = '"..amplefi.DbValue(data.suite4).."',"
   sql = sql.."location4 = '"..amplefi.DbValue(data.location4).."',"
   sql = sql.."city4 = '"..data.city4.."',"
   sql = sql.."state4 = '"..data.state4.."',"
   sql = sql.."zip4 = '"..data.zip4.."',"
   sql = sql.."phone4 = '"..data.phone4.."',"
   sql = sql.."fax4 = '"..data.fax4.."',"
   sql = sql.."protocol4 = '"..data.protocol4.."',"
   sql = sql.."groupname5 = '"..amplefi.DbValue(data.groupname5).."',"
   sql = sql.."facility5 = '"..amplefi.DbValue(data.facility5).."',"
   sql = sql.."street5 = '"..amplefi.DbValue(data.street5).."',"
   sql = sql.."suite5 = '"..amplefi.DbValue(data.suite5).."',"
   sql = sql.."location5 = '"..amplefi.DbValue(data.location5).."',"
   sql = sql.."city5 = '"..data.city5.."',"
   sql = sql.."state5 = '"..data.state5.."',"
   sql = sql.."zip5 = '"..data.zip5.."',"
   sql = sql.."phone5 = '"..data.phone5.."',"
   sql = sql.."fax5 = '"..data.fax5.."',"
   sql = sql.."protocol5 = '"..data.protocol5.."',"
   sql = sql.."groupname6 = '"..amplefi.DbValue(data.groupname6).."',"
   sql = sql.."facility6 = '"..amplefi.DbValue(data.facility6).."',"
   sql = sql.."street6 = '"..amplefi.DbValue(data.street6).."',"
   sql = sql.."suite6 = '"..amplefi.DbValue(data.suite6).."',"
   sql = sql.."location6 = '"..amplefi.DbValue(data.location6).."',"
   sql = sql.."city6 = '"..data.city6.."',"
   sql = sql.."state6 = '"..data.state6.."',"
   sql = sql.."zip6 = '"..data.zip6.."',"
   sql = sql.."phone6 = '"..data.phone6.."',"
   sql = sql.."fax6 = '"..data.fax6.."',"
   sql = sql.."protocol6 = '"..data.protocol6.."',"
   sql = sql.."school1 = '"..amplefi.DbValue(data.school1).."',"
   sql = sql.."school2 = '"..amplefi.DbValue(data.school2).."',"
   sql = sql.."school3 = '"..amplefi.DbValue(data.school3).."',"
   sql = sql.."residency1 = '"..amplefi.DbValue(data.residency1).."',"
   sql = sql.."residency2 = '"..amplefi.DbValue(data.residency2).."',"
   sql = sql.."residency3 = '"..amplefi.DbValue(data.residency3).."',"
   sql = sql.."residency4 = '"..amplefi.DbValue(data.residency4).."',"
   sql = sql.."residency5 = '"..amplefi.DbValue(data.residency5).."',"
   sql = sql.."residency6 = '"..amplefi.DbValue(data.residency6).."',"
   sql = sql.."residency7 = '"..amplefi.DbValue(data.residency7).."',"
   sql = sql.."residency8 = '"..amplefi.DbValue(data.residency8).."',"
   sql = sql.."residency9 = '"..amplefi.DbValue(data.residency9).."',"
   sql = sql.."fellowship1 = '"..amplefi.DbValue(data.fellowship1).."',"
   sql = sql.."fellowship2 = '"..amplefi.DbValue(data.fellowship2).."',"
   sql = sql.."fellowship3 = '"..amplefi.DbValue(data.fellowship3).."',"
   sql = sql.."fellowship4 = '"..amplefi.DbValue(data.fellowship4).."',"
   sql = sql.."fellowship5 = '"..amplefi.DbValue(data.fellowship5).."',"
   sql = sql.."fellowship6 = '"..amplefi.DbValue(data.fellowship6).."',"
   sql = sql.."fellowship7 = '"..amplefi.DbValue(data.fellowship7).."',"
   sql = sql.."fellowship8 = '"..amplefi.DbValue(data.fellowship8).."',"
   sql = sql.."fellowship9 = '"..amplefi.DbValue(data.fellowship9).."',"
   sql = sql.."templetitle = '"..amplefi.DbValue(data.templetitle).."',"
   sql = sql.."templearea = '"..amplefi.DbValue(data.templearea).."',"
   sql = sql.."templeinstitution = '"..amplefi.DbValue(data.templeinstitution).."',"
   sql = sql.."drexeltitle = '"..amplefi.DbValue(data.drexeltitle).."',"
   sql = sql.."drexelarea = '"..amplefi.DbValue(data.drexelarea).."',"
   sql = sql.."drexelinstitution = '"..amplefi.DbValue(data.drexelinstitution).."',"
   sql = sql.."title1 = '"..amplefi.DbValue(data.title1).."',"
   sql = sql.."title2 = '"..amplefi.DbValue(data.title2).."',"
   sql = sql.."title3 = '"..amplefi.DbValue(data.title3).."',"
   sql = sql.."parentorg = '"..amplefi.DbValue(data.parentorg).."',"
   sql = sql.."employed = '"..amplefi.DbValue(data.employed).."',"
   sql = sql.."agh_7 = '"..amplefi.DbValue(data.agh_7).."',"
   sql = sql.."avh_5 = '"..amplefi.DbValue(data.avh_5).."',"
   sql = sql.."cgh_6 = '"..amplefi.DbValue(data.cgh_6).."',"
   sql = sql.."frh_9 = '"..amplefi.DbValue(data.frh_9).."',"
   sql = sql.."jeff_11 = '"..amplefi.DbValue(data.jeff_11).."',"
   sql = sql.."stv_8 = '"..amplefi.DbValue(data.stv_8).."',"
   sql = sql.."wph_10 = '"..amplefi.DbValue(data.wph_10).."',"
   sql = sql.."clinicalexpertise1 = '"..amplefi.DbValue(data.clinicalexpertise1).."',"
   sql = sql.."clinicalexpertise2 = '"..amplefi.DbValue(data.clinicalexpertise2).."',"
   sql = sql.."clinicalexpertise3 = '"..amplefi.DbValue(data.clinicalexpertise3).."',"
   sql = sql.."clinicalexpertise4 = '"..amplefi.DbValue(data.clinicalexpertise4).."',"
   sql = sql.."clinicalexpertise5 = '"..amplefi.DbValue(data.clinicalexpertise5).."',"
   sql = sql.."clinicalexpertise6 = '"..amplefi.DbValue(data.clinicalexpertise6).."',"
   sql = sql.."clinicalexpertise7 = '"..amplefi.DbValue(data.clinicalexpertise7).."',"
   sql = sql.."clinicalexpertise8 = '"..amplefi.DbValue(data.clinicalexpertise8).."',"
   sql = sql.."clinicalexpertise9 = '"..amplefi.DbValue(data.clinicalexpertise9).."',"
   sql = sql.."clinicalexpertise10 = '"..amplefi.DbValue(data.clinicalexpertise10).."',"
   sql = sql.."clinicalexpertise11 = '"..amplefi.DbValue(data.clinicalexpertise11).."',"
   sql = sql.."clinicalexpertise12 = '"..amplefi.DbValue(data.clinicalexpertise12).."',"
   sql = sql.."clinicalexpertise13 = '"..amplefi.DbValue(data.clinicalexpertise13).."',"
   sql = sql.."clinicalexpertise14 = '"..amplefi.DbValue(data.clinicalexpertise14).."',"
   sql = sql.."clinicalexpertise15 = '"..amplefi.DbValue(data.clinicalexpertise15).."',"
   sql = sql.."clinicalexpertise16 = '"..amplefi.DbValue(data.clinicalexpertise16).."',"
   sql = sql.."clinicalexpertise17 = '"..amplefi.DbValue(data.clinicalexpertise17).."',"
   sql = sql.."clinicalexpertise18 = '"..amplefi.DbValue(data.clinicalexpertise18).."',"
   sql = sql.."clinicalexpertise19 = '"..amplefi.DbValue(data.clinicalexpertise19).."',"
   sql = sql.."clinicalexpertise20 = '"..amplefi.DbValue(data.clinicalexpertise20).."',"
   sql = sql.."clinicalexpertise21 = '"..amplefi.DbValue(data.clinicalexpertise21).."',"
   sql = sql.."clinicalexpertise22 = '"..amplefi.DbValue(data.clinicalexpertise22).."',"
   sql = sql.."clinicalexpertise23 = '"..amplefi.DbValue(data.clinicalexpertise23).."',"
   sql = sql.."clinicalexpertise24 = '"..amplefi.DbValue(data.clinicalexpertise24).."',"
   sql = sql.."clinicalexpertise25 = '"..amplefi.DbValue(data.clinicalexpertise25).."',"
   sql = sql.."ntwkalign = '"..amplefi.DbValue(data.ntwkalign).."',"   
   local location = ''
   local params = {}
   local lat = ''
   local lng = ''
   if tostring(data.street1):upper() ~= tostring(dbdata[1].street1:nodeValue()):upper() then
      location = data.street1..' '..data.city1..' '..data.state1..' '..data.zip1
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat,lng = amplefi.GetLatLong(params)
      if lat ~= '' then
         sql = sql.."latitude1 = '"..lat.."',"
         sql = sql.."longitude1 = '"..lng.."',"
      end
   end
   lat = ''
   lng = ''
   if tostring(data.street2):upper() ~= dbdata[1].street2:nodeValue():upper() 
      and tostring(data.street2):upper() ~= '' then
      location = data.street2..' '..data.city2..' '..data.state2..' '..data.zip2
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat,lng = amplefi.GetLatLong(params)
      if lat ~= '' then
         sql = sql.."latitude2 = '"..lat.."',"
         sql = sql.."longitude2 = '"..lng.."',"
      end
--   else
--      sql = sql.."latitude2 = '',"
--      sql = sql.."longitude2 = '',"
   end
   lat = ''
   lng = ''
   if tostring(data.street3):upper() ~= dbdata[1].street3:nodeValue():upper() 
      and tostring(data.street3):upper() ~= '' then
      location = data.street3..' '..data.city3..' '..data.state3..' '..data.zip3
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat,lng = amplefi.GetLatLong(params)
      if lat ~= '' then
         sql = sql.."latitude3 = '"..lat.."',"
         sql = sql.."longitude3 = '"..lng.."',"
      end
--   else
--      sql = sql.."latitude3 = '',"
--      sql = sql.."longitude3 = '',"
   end
   lat = ''
   lng = ''
   if tostring(data.street4):upper() ~= dbdata[1].street4:nodeValue():upper()
      and tostring(data.street4):upper() ~= '' then
      location = data.street4..' '..data.city4..' '..data.state4..' '..data.zip4
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat,lng = amplefi.GetLatLong(params)
      if lat ~= '' then
         sql = sql.."latitude4 = '"..lat.."',"
         sql = sql.."longitude4 = '"..lng.."',"
      end
--   else
--      sql = sql.."latitude4 = '',"
--      sql = sql.."longitude4 = '',"
   end
   lat = ''
   lng = ''
   if tostring(data.street5):upper() ~= dbdata[1].street5:nodeValue():upper()
      and tostring(data.street5):upper() ~= '' then
      location = data.street5..' '..data.city5..' '..data.state5..' '..data.zip5
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat,lng = amplefi.GetLatLong(params)
      if lat ~= '' then
         sql = sql.."latitude5 = '"..lat.."',"
         sql = sql.."longitude5 = '"..lng.."',"
      end
--   else
--      sql = sql.."latitude5 = '',"
--      sql = sql.."longitude5 = '',"
   end
   lat = ''
   lng = ''
   if tostring(data.street6):upper() ~= dbdata[1].street6:nodeValue():upper()
      and tostring(data.street6):upper() ~= '' then
      location = data.street6..' '..data.city6..' '..data.state6..' '..data.zip6
      params = {
         ['source']='GoogleV3',
         ['location']=location,
      }
      lat,lng = amplefi.GetLatLong(params)
      if lat ~= '' then
         sql = sql.."latitude6 = '"..lat.."',"
         sql = sql.."longitude6 = '"..lng.."',"
      end
--   else
--      sql = sql.."latitude6 = '',"
--      sql = sql.."longitude6 = '',"
   end
   sql = sql.."keywords1 = '"..amplefi.DbValue(data.keywords1).."',"
   if data.dermoncall == '' then
      sql = sql.."dermoncall = 'False',"
   else
      sql = sql.."dermoncall = '"..amplefi.DbValue(data.dermoncall).."',"
   end
   if data.zocdoc == '' then
      sql = sql.."zocdoc = 'False',"
   else
      sql = sql.."zocdoc = '"..amplefi.DbValue(data.zocdoc).."',"
   end
   if data.epicosenabled == '' then
      sql = sql.."epicosenabled = 'False',"
   else
      sql = sql.."epicosenabled = '"..amplefi.DbValue(data.epicosenabled).."',"
   end
   sql = sql.."epicvisittype = '"..amplefi.DbValue(data.epicvisittype).."' "
   sql = sql.."WHERE npi = '"..data.npi.."'"
   trace(sql)
   if not iguana.isTest() then
      local result = amplefi.ExecuteMySql(sql)
   end
   --sql = sql:gsub("ahn.Golden","ahn.ref.Golden")
   --result = amplefi.ExecuteMsSql(sql)
end

function MxQualityExists(cmsno)
   local sql = "SELECT * FROM ahn.service_quality WHERE cmsno = '"..cmsno.."';"
   local ret = amplefi.ExecuteMySql(sql)
   local tmp = false
   if #ret > 0 then
      tmp = true
   end
   return tmp,ret
end

function MxProviderExists(mxname)
   local sql = "SELECT * FROM ahn.service_providers WHERE service_type = 'Med Express' "
   sql = sql.."AND provider_name = '"..mxname.."';"
   local ret = amplefi.ExecuteMySql(sql)
   local tmp = false
   if #ret > 0 then
      tmp = true
   end
   return tmp,ret
end


function BioExists(npi)
   local sql = "SELECT * FROM ahn.GoldenBioList WHERE npi = '"..npi.."'"
   local ret = amplefi.ExecuteMySql(sql)
   local tmp = false
   if #ret > 0 then
      tmp = true
   end
   return tmp,ret
end

function DoctorExists(npi)
   local sql = "SELECT * FROM ahn.GoldenProviderList WHERE npi = '"..npi.."'"
   local ret = amplefi.ExecuteMySql(sql)
   local tmp = false
   if #ret > 0 then
      tmp = true
   end
   return tmp,ret
end

function ProcessLocationList(Data)
   local ix1,ix2 = Data:find('"quality":')
   local iy1,iy2 = Data:find(',"listname"')
   local qstr = Data:sub(ix2+1,iy1-1)
   --trace(qstr)
   local jsn = json.parse{data=Data}
   local cmsno,prec = AhnServiceProviderExists(
      jsn.provider.name,
      jsn.provider.addr1,jsn.provider.city,
      jsn.provider.state,jsn.provider.zip)
   --trace(cmsno)
   if cmsno == '' then
      trace('insert')
      local newcmsno = GetNextAmpCmsNo()
      InsertAhnServiceProvider(jsn.provider,newcmsno)
      InsertAhnServiceQuality(qstr,newcmsno)
   else
      trace('update')
      UpdateAhnServiceProvider1(cmsno,jsn,prec)
      UpdateAhnServiceQuality(cmsno,qstr)
   end
end

function InsertAhnServiceProvider(providerinfo,cmsno)
   local sql = "INSERT INTO ahn.service_providers ("
   sql = sql.."cmsno,state,provider_name,address,city,"
   sql = sql.."zip,phone,service_type,latitude,longitude,"
   sql = sql.."hh_partner,adjacent,pin_color) VALUES ('"..cmsno.."'"
   sql = sql..",'"..providerinfo.state.."','"
   sql = sql..amplefi.DbValue(providerinfo.name).."','"
   sql = sql..amplefi.DbValue(providerinfo.addr1).."','"
   sql = sql..providerinfo.city.."','"
   sql = sql..providerinfo.zip.."','"
   sql = sql..providerinfo.phone.."','"
   sql = sql..providerinfo.service_type.."','"
   sql = sql..providerinfo.latitude.."','"
   sql = sql..providerinfo.longitude.."','"
   sql = sql..providerinfo.hh_partner.."','"
   sql = sql..providerinfo.adjacent.."','GREEN')"
   --trace(sql)
   local result = amplefi.ExecuteMySql(sql)
end

function InsertAhnServiceQuality(qualityinfo,cmsno)
   local sql = "INSERT INTO ahn.service_quality ("
   sql = sql.."criteria,criteria_type,cmsno,answer) "
   sql = sql.."VALUES ('Display','service','"..cmsno
   sql = sql.."','"..amplefi.DbValue(qualityinfo).."')"
   --trace(sql)
   local result = amplefi.ExecuteMySql(sql)
end

function GetNextAmpCmsNo()
   local lastcmsno = ''
   local newcmsno = ''
   local num = 0
   local sql = "SELECT MAX(cmsno) AS maxcmsno "
   sql = sql.."FROM ahn.service_providers "
   sql = sql.."WHERE cmsno LIKE 'AMP%'"
   local result = amplefi.ExecuteMySql(sql)
   if #result ~= 0 then
      lastcmsno = result[1].maxcmsno
      num = tostring(lastcmsno):sub(-5) + 1
      newcmsno = 'AMP'..('00000'..num):sub(-5)
   end
   return newcmsno
end

function UpdateAhnServiceQuality(cmsno,qstr)
   local sql = "UPDATE ahn.service_quality SET answer = '"..qstr.."' "
   sql = sql.."WHERE cmsno = '"..cmsno.."' AND criteria = 'Display' "
   sql = sql.."AND criteria_type = 'service'"
   --trace(sql)
   local result = amplefi.ExecuteMySql(sql)
end

function UpdateAhnServiceProvider1(cmsno,jsn,rec)
   local sql = "UPDATE ahn.service_providers SET "
   sql = sql.."provider_name = '"..amplefi.DbValue(jsn.provider.name).."'"
   sql = sql..",address = '"..amplefi.DbValue(jsn.provider.addr1).."'"
   sql = sql..",city = '"..jsn.provider.city.."'"
   sql = sql..",state = '"..jsn.provider.state.."'"
   sql = sql..",zip = '"..jsn.provider.zip.."'"
   sql = sql..",phone = '"..jsn.provider.phone.."'"
   sql = sql..",latitude = '"..jsn.provider.latitude.."'"
   sql = sql..",longitude = '"..jsn.provider.longitude.."'"
   local params = {
      ['state']=jsn.provider.state:upper(),
      ['lat']=jsn.provider.latitude,
      ['lng']=jsn.provider.longitude
   }
   local distance,state = amplefi.GetLocalizer(params)
   if distance ~= nil then
      sql = sql..",localizer = "..distance
   else
      sql = sql..",localizer = 0"
   end
   sql = sql.." WHERE cmsno = '"..cmsno.."'"
   --trace(sql)
   local result = amplefi.ExecuteMySql(sql)
end

function AhnServiceProviderExists(ProviderName,Address,City,State,Zip)
   local cmsno = ''
   local sql = "SELECT * FROM ahn.service_providers WHERE provider_name = '"
   sql = sql..amplefi.DbValue(ProviderName).."' AND address = '"
   sql = sql..amplefi.DbValue(Address).."' AND city = '"..City.."' AND "
   sql = sql.."state = '"..State.."' AND zip = '"..Zip.."'"
   local result = amplefi.ExecuteMySql(sql)
   --trace(result)
   if #result ~= 0 then
      cmsno = tostring(result[1].cmsno)
   end
   return cmsno,result
end
--[[
function UpdateAhnServiceProvider(cmsno,jsn,rec)
   local addrChanged = false
   local sql = "UPDATE ahn.service_providers SET "
   sql = sql.."provider_name = '"..amplefi.DbValue(jsn.provider.name).."'"
   if jsn.provider.addr1:upper() ~= (tostring(rec[1].address)):upper() then
      addrChanged = true
      sql = sql..",address = '"..amplefi.DbValue(jsn.provider.addr1).."'"
   end
   if jsn.provider.city:upper() ~= (tostring(rec[1].city)):upper() then
      addrChanged = true
      sql = sql..",city = '"..jsn.provider.city.."'"
   end
   if jsn.provider.state:upper() ~= (tostring(rec[1].state)):upper() then
      addrChanged = true
      sql = sql..",state = '"..jsn.provider.state.."'"
   end
   if jsn.provider.zip:upper() ~= (tostring(rec[1].zip)):upper() then
      addrChanged = true
      sql = sql..",zip = '"..jsn.provider.zip.."'"
   end
   if addrChanged then
      trace('update lat/lng')
      local location = jsn.provider.addr1..' '..jsn.provider.city..' '
      location = location..jsn.provider.state..' '..jsn.provider.zip
      local params = {
         ['location']=location
      }
      local lat,lng = amplefi.GetLatLong(params)
      if lat ~= nil then
         sql = sql..",latitude = '"..lat.."'"
         sql = sql..",longitude = '"..lng.."'"
      end
      params = {
         ['state']=jsn.provider.state:upper(),
         ['lat']=lat,
         ['lng']=lng
      }
      local distance,state = amplefi.GetLocalizer(params)
      if distance ~= nil then
         sql = sql..",localizer = "..distance
      end
   end
   sql = sql.." WHERE cmsno = '"..cmsno.."'"
   local result = amplefi.ExecuteMySql(sql)
end
--]]

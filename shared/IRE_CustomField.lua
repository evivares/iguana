
IREcustfld = {}

function IREcustfld.getValue(T)
   if type(T.dstRule.Xref[T.dataGUID].ValueDef) ~= 'table' then
      error("Missing ValueDef parameter for CustomField:"..T.dataGUID.." in Rule:"..T.dstRule.IREmsh.Rule_GUID, 4)
   end
   
   local def = T.dstRule.Xref[T.dataGUID].ValueDef
   local val
   
   for k,v in pairs(def) do
      val = IREcustfld[k](v, T.srcRule, T.dstRule, T.dataTable)
   end
   
   return val
end

function IREcustfld.Concat(T, Src, Dst, Data)
   if not T.Type then T.Type = 'Repeats' end
   if not T.Separator then T.Separator = '' end
   if not T.Data or T.Data == '' then error("CustomField 'Concat' has no 'Data' parameter value") end
   T.Type = T.Type:upper()
   local val = ''
   
   if T.Type == 'CHILDREN' then
      for i=1, #T.Data do
         local e,f = next(T.Data[i])
         local tmp
         if e:upper() == 'SOURCE' then
            -- Get list of child Data_GUIDs and process them all
            --IRErs.getChildGUIDs(GUID, rule)
            local guids = IRErs.getChildGUIDs(T.Data[i].Source.Data_GUID, Src)
            for j=1, #guids do
               if Src.DataTypes[Src.Xref[guids[j]].DataType].isArray == false then
                  tmp = IREcustfld[e]({Data_GUID=guids[j]},Src,Dst,Data)
                  if tmp and tmp ~= '' then
                     if val ~= '' then val = val..T.Separator end
                     val = val..tostring(tmp)
                  end
               end
            end
         else
            tmp = IREcustfld[e](f,Src,Dst,Data)
            if tmp and tmp ~= '' then
               if val ~= '' then val = val..T.Separator end
               val = val..tostring(tmp)
            end
         end
         
      end
   elseif T.Type == 'REPEATS' or T.Type == 'LIST' then
      for i=1, #T.Data do
         local e,f = next(T.Data[i])
         local limit1 = false
         if T.Type == 'LIST' then limit1 = true end
         local tmp = IREcustfld[e](f,Src,Dst,Data, limit1)
         if type(tmp) == 'table' then
            for j=1, #tmp do
               if tmp[i].val and tmp[i].val ~= '' then
                  if val ~= '' then val = val..T.Separator end
                  val = val..tostring(tmp[i].val)
               end
            end
         else
            if tmp and tmp ~= '' then
               if val ~= '' then val = val..T.Separator end
               val = val..tostring(tmp)
            end
         end
      end
   end
   
   return val
end

function IREcustfld.Condition(T, Src, Dst, Data)
   local val
   if #T > 0 then
      for k,v in pairs(T) do
         val = IREcustfld.testCondition(v, Src, Dst, Data)
         if val then break end
      end
   else
      val = IREcustfld.testCondition(T, Src, Dst, Data)
   end
   
   if type(val) == 'table' then
      local e,f = next(val)
      val = IREcustfld[e](f,Src,Dst,Data)
   end
   
   return val
end

function IREcustfld.testCondition(T, Src, Dst, Data)
   if type(T.Val_A) == 'table' then
      local e,f = next(T.Val_A)
      T.Val_A = IREcustfld[e](f,Src,Dst,Data)
   end
   if type(T.Val_B) == 'table' then
      local e,f = next(T.Val_B)
      T.Val_B = IREcustfld[e](f,Src,Dst,Data)
   end
   if T.Case:lower() == 'insensitive' or T.Case:lower() == 'i' then
      T.Val_A = T.Val_A and T.Val_A:upper()
      T.Val_B = T.Val_B and T.Val_B:upper()
   end
   
   local res
   
   if T.Mode:lower() == '=' or 'equal' then
      res = T.Val_A == T.Val_B and true or false
   elseif T.Mode:lower() == '~=' or '<>' or 'notequal' then
      res = T.Val_A ~= T.Val_B and true or false
   elseif T.Mode:lower() == '<' or 'lessthan' then
      res = T.Val_A < T.Val_B and true or false
   elseif T.Mode:lower() == '>' or 'greaterthan' then
      res = T.Val_A > T.Val_B and true or false
   elseif T.Mode:lower() == '<=' or '=<' or 'lessthanorequal' then
      res = T.Val_A <= T.Val_B and true or false
   elseif T.Mode:lower() == '>=' or '=>' or 'greaterthanorequal' then
      res = T.Val_A >= T.Val_B and true or false
   elseif T.Mode:lower() == 'allelse' or 'else' or 'unknown' then
      res = true
   end
   -- ensure nil if value is not nil and ''
   T.True = not T.True and T.True or T.True ~= '' and T.True or nil
   T.False = not T.False and T.False or T.False ~= '' and T.False or nil
   
   return res and T.True or T.False
end

function IREcustfld.Source(T,Src,Dst,Data,limit1)
   --dataTable=T.data, dataGUID=k, valType=T.valueType, srcRule=T.srcRule, dstRule=T.dstRule
   local val = IREproc.extractData{dataTable=Data, dataGUID=T.Data_GUID, valType=IREproc.valueType.Actual, srcRule=Src, dstRule=Dst}
   
   if limit1 == false then
      return val
   else
      return val[1] and val[1].val or nil
   end
end

function IREcustfld.Static(T, Src, Dst, Data)
	local val   
   if type(T) == 'string' then
      T = T:trimWS()
      if T:match('%%.+%%') then
         if T == '%SQLNULL%' then
            val = 'NULL'
         elseif T == '%OSDATETIME%' then            
            error("Unknown static:"..T)
         elseif T == '%CLIENT_ID%' then
            if not IREcustfld.Client_ID then error("IREcustfld.Client_ID is required!", 7) end
            val = IREcustfld.Client_ID
         elseif T == '%SQLNOWDT%' or '%SQLDATETIME%' then
            if not IREcustfld.SQLtype then error("IREcustfld.SQLtype as db.<type> is required!", 7) end
            if IREcustfld.SQLtype == db.SQL_SERVER then
               val = 'GETDATE()'
            elseif IREcustfld.SQLtype == db.MY_SQL then
               val = 'NOW()'
            elseif IREcustfld.SQLtype == db.SQLITE then
               return "datetime('now')"
            else
               error("Unknown static:"..T)
            end
         else
            error("Unknown static:"..T)
         end
      else
         val = T
      end
   end
   return val
end
require 'IRE_RuleStore'
require 'IRE_CustomField'

IREproc = {}

-- Allow empty field data output
IREproc.allowEmpty = false

IREproc.valueType = {
   Actual = 1001,
   ParentGUID = 1002,
   EmptySet = 1003}

function IREproc.flowToDataTable(T)
   --{srcRuleGUID='',dstRuleGUID='',data=srcMsg}
   if not T.SourceRule_GUID then error("SourceRule_GUID required!", 2) end
   if not T.DestRule_GUID then error("DestRule_GUID required!", 2) end
   if not T.data then error("data required!", 2) end
   
   local srcRule = IRErs.getRuleAsTable{ruleGUID=T.SourceRule_GUID}
   local dstRule = IRErs.getRuleAsTable{ruleGUID=T.DestRule_GUID}
   local data = {}
   
   if not srcRule then error("No source rule to process this message!") end
   if not dstRule then error("No destination rule to process this message!") end
   
   data = IREproc.flowRulesToDataTable{srcRule=srcRule, dstRule=dstRule, data=T.data}
   
   return data
end

function IREproc.flowRulesToDataTable(T)
   --{srcRule=srcRule, dstRule=dstRule, data=T.data}
   if not T.srcRule then error("srcRule is empty!", 2) end
   if not T.dstRule then error("dstRule is empty!", 2) end
   if not T.valueType then T.valueType = IREproc.valueType.Actual end
   if T.data == nil and T.valueType == IREproc.valueType.Actual then error("data is empty!", 2) end
   
   local data = {}
   
   for k,v in pairs(T.dstRule.Xref) do
      if T.dstRule.DataTypes[v.DataType].isArray == false then
         local valLocArr 
         valLocArr = IREproc.getValueLocationArray{dataTable=T.data, dataGUID=k, 
            valType=T.valueType, dstRule=T.dstRule, srcRule=T.srcRule}
         
         for i=1, #valLocArr do
            data = IREproc.insertTblGUID{
               fullTbl = data,
               ruleTable = T.ruleTable,
               dataGUID = k,
               location = valLocArr[i].loc,
               value = valLocArr[i].val}
         end
   
      end
   end

   return data
end

function IREproc.ruleToDataTable(T)
   if T.ruleTable == nil then error("ruleTable is empty!", 2) end
   if T.ruleTable.Xref == nil then error("ruleTable is empty!", 2) end
   if not T.valueType then T.valueType = IREproc.valueType.Actual end
   if T.data == nil and T.valueType == IREproc.valueType.Actual then error("data is empty!", 2) end
   --[[
   ruleTable = IRErule,
   data=nil,
   valueType = IREproc.valueType.ParentGUID
   ]]--
   
   local data = {}
   
   for k,v in pairs(T.ruleTable.Xref) do
      if T.ruleTable.DataTypes[v.DataType].isArray == false then
         --valLocArr = IREproc.getValueLocationArray{dataTable=T.data, dataGUID=k,valType=T.valueType, dstRule=T.dstRule, srcRule=T.srcRule}
         local valLocArr = IREproc.getValueLocationArray{dataTable=T.data, dataGUID=k, valType=T.valueType, dstRule=T.ruleTable, srcRule=T.ruleTable}
         
         for i=1, #valLocArr do
            data = IREproc.insertTblGUID{
               fullTbl = data,
               ruleTable = T.ruleTable,
               dataGUID = k,
               location = valLocArr[i].loc,
               value = valLocArr[i].val}
         end
   
      end
   end

   return data
end

function IREproc.getValueLocationArray(T)
   
   --[[ Returns repeating table of {value, repeat-index}
   If value does not repeat, it is a single table entry and repeat-index=0
   This is required due to dataGUID identifying a location that can repeat.
   This repeat can have other relevant dataGUID's
   at the same index in the source data. ]]--
   local outValIdx = IREproc.extractData{dataTable=T.dataTable, dataGUID=T.dataGUID,
      valType=T.valType, srcRule=T.srcRule, dstRule=T.dstRule}
   

   --[[ Returns a table of location and repeats locations in the destination table
   locData.loc is a table of parent element keys
   locData.rpts is a table of indexes in locData.loc where repeats occur
   This combination allows correlation directly to specific repeats in locData.loc 
   NOTE: location data and repeats indexes are in reverse order!! ]]--
   -- local locData = IREproc.getDataLocation(T.ruleTable, T.dataGUID)

   for i=1, #outValIdx do
      local dtx = IREproc.getDataLocation(T.dstRule, T.dataGUID)
      
      --if #outValIdx[i].idx < #dtx.rpts then error("Unable to determine repeat index!", 4) end

      -- process list of repeats in reverse
      for j=#dtx.rpts,1,-1 do
         -- Zero index array location
         local ZeroIdx = #dtx.rpts-j

         local srcIdx = outValIdx[i].idx[ZeroIdx+1]
         
         if #outValIdx[i].idx < ZeroIdx+1 then
            srcIdx = 1
         end

         -- Index within list of parent element keys where we alter the index
         local thsIdx = dtx.rpts[j]

         -- Number of repeats indexes in the source data for this value
         local srcIdxCnt = #outValIdx[i].idx

         -- Determine if number of source indexes is greater than the number in destination
         -- Do some math on the last source repeat
         if j == 1 and srcIdxCnt-ZeroIdx > j then
            -- Source has more repeats
            --multiply them to determine the destination index
            for k=ZeroIdx+2, srcIdxCnt do
               srcIdx = srcIdx * outValIdx[i].idx[k]
            end
         end
         dtx.loc[thsIdx] = srcIdx
      end
      outValIdx[i].loc = dtx.loc
   end

   return outValIdx
end
   
function IREproc.insertTblGUID(T)
   --[[
   fullTbl = data,
   ruleTable = T.ruleTable,
   dataGUID = k
   location = valueLoc
   value = outVal}
   ]]--
   
   --IREproc.validateLocation(fullTbl, location)
   T.fullTbl = IREproc.validateLocation(T.fullTbl, T.location)
   
   local GUIDs = T.location
   if #GUIDs == 1 then
      T.fullTbl[GUIDs[#GUIDs]] = T.value
   elseif #GUIDs == 2 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]] = T.value
   elseif #GUIDs == 3 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]] = T.value
   elseif #GUIDs == 4 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]] = T.value
   elseif #GUIDs == 5 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]] = T.value
   elseif #GUIDs == 6 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]] = T.value
   elseif #GUIDs == 7 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]] = T.value
   elseif #GUIDs == 8 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]] = T.value
   elseif #GUIDs == 9 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]][GUIDs[#GUIDs-8]] = T.value
   elseif #GUIDs == 10 then
      T.fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]][GUIDs[#GUIDs-8]][GUIDs[#GUIDs-9]] = T.value
   else
      error("More than 10 layers identified! Add more layers...", 2)
   end
   
   return T.fullTbl
end

function IREproc.validateLocation(fullTbl, GUIDs)
   if #GUIDs > 1 then
      if fullTbl[GUIDs[#GUIDs]] == nil then
         fullTbl[GUIDs[#GUIDs]] = {}
      end
   end
   if #GUIDs > 2 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]] = {}
      end
   end
   if #GUIDs > 3 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]] = {}
      end
   end
   if #GUIDs > 4 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]] = {}
      end
   end
   if #GUIDs > 5 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]] = {}
      end
   end
   if #GUIDs > 6 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]] = {}
      end
   end
   if #GUIDs > 7 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]] = {}
      end
   end
   if #GUIDs > 8 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]] = {}
      end
   end
   if #GUIDs > 9 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]][GUIDs[#GUIDs-8]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]][GUIDs[#GUIDs-8]] = {}
      end
   end
   if #GUIDs > 10 then
      if fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]][GUIDs[#GUIDs-8]][GUIDs[#GUIDs-9]] == nil then
         fullTbl[GUIDs[#GUIDs]][GUIDs[#GUIDs-1]][GUIDs[#GUIDs-2]][GUIDs[#GUIDs-3]][GUIDs[#GUIDs-4]][GUIDs[#GUIDs-5]][GUIDs[#GUIDs-6]][GUIDs[#GUIDs-7]][GUIDs[#GUIDs-8]][GUIDs[#GUIDs-9]] = {}
      end
   end
   if #GUIDs > 11 then
      error("validateLocation - Exceeds depth of 11! Add more layers...")
   end
   
   return fullTbl
end

function IREproc.getDataLocation(ruleTbl, dataGUID, valueType)
   local data = {loc={},rpts={}}
   
   if ruleTbl.IREmsh.Message_Type_GUID == nil or ruleTbl.IREmsh.Message_Type_GUID == '' then error("Unable to determine Message_Type_GUID!", 3) end
   
   if tonumber(ruleTbl.Xref[dataGUID].RPT) ~= 0 then
      data.loc[#data.loc+1] = -1
      data.rpts[#data.rpts+1] = #data.loc
   end
   data.loc[#data.loc+1] = IREproc.getKeyName(ruleTbl, dataGUID, valueType)
   
   while dataGUID ~= ruleTbl.IREmsh.Message_Type_GUID do
      if ruleTbl.Xref[dataGUID].Parent_GUID == ruleTbl.IREmsh.Message_Type_GUID then
         break
      end
      
      if dataGUID == ruleTbl.Xref[dataGUID].Parent_GUID then
         error("Infinite loop prevented! Parent_GUID matches Data_GUID:"..dataGUID.." in rule:"..ruleTbl.IREmsh.Rule_GUID)
      end
      
      local parentGUID = ruleTbl.Xref[dataGUID].Parent_GUID
      
      if not ruleTbl.Xref[parentGUID] or not ruleTbl.Xref[parentGUID].Parent_GUID then error("Identified orphaned child Data_GUID: "..dataGUID) end
      
      if tonumber(ruleTbl.Xref[parentGUID].RPT) ~= 0 then
         --trace(#data.loc)
         data.loc[#data.loc+1] = -1
         data.rpts[#data.rpts+1] = #data.loc
      end
      data.loc[#data.loc+1] = IREproc.getKeyName(ruleTbl, parentGUID, valueType)
      
      dataGUID = parentGUID
   end
   return data
end

function IREproc.extractData(T)
   --dataTable=T.data, dataGUID=k, valType=T.valueType, srcRule=T.srcRule, dstRule=T.dstRule
   
   if T.valType == IREproc.valueType.EmptySet then
      return {{val = '', idx = {1}}}
   end
   if T.valType == IREproc.valueType.ParentGUID then
      local val = IREproc.getDataLocation(T.dstRule, T.dataGUID, IRErs.elementKeyType.dataGUID).loc
      return {{val = table.concat(val,','), idx = {1}}}
   end
   if T.valType == IREproc.valueType.Actual then
      if T.srcRule.Xref[T.dataGUID] then
         if T.srcRule.IREmsh.Format == 'HL7' then
            return IREproc.getHL7Data{
               hl7Location = T.srcRule.Xref[T.dataGUID].Data_Location, 
               required = T.dstRule.Xref[T.dataGUID] and T.dstRule.Xref[T.dataGUID].REQ or false, 
               data = T.dataTable}
         elseif T.srcRule.IREmsh.Format == 'CSV' then
            error("Format identified in rule 'IREmsh.Format' has not been implemented!", 4)
         elseif T.srcRule.IREmsh.Format == 'XML' then
            error("Format identified in rule 'IREmsh.Format' has not been implemented!", 4)
         else
            error("Format identified in rule 'IREmsh.Format' has not been implemented!", 4)
         end
      else
         if T.dstRule.Xref[T.dataGUID].DataType == 'CustomField' then
            return {{idx={}, val=IREcustfld.getValue(T)}}
         end   
      end
   end
end

function IREproc.getHL7Data(T)
   --hl7Location, required, data
   local location = T.hl7Location:split(',')
   local data = T.data
   local dataRpt = {}
   
   for i=1, #location do
      local rpt = false
      if location[i]:find('<') ~= nil and location[i]:find('>') ~= nil then
         rpt = true
         location[i] = location[i]:match('%d+')
      end
      if tonumber(location[i]) ~= nil then location[i] = tonumber(location[i]) end
      --trace(rpt, #data, data:isLeaf(), data:isNull())
      --trace(data[location[i]]:isLeaf(), data[location[i]]:isNull())
      if data[location[i]]:isNull() then
         if IREproc.allowEmpty and T.required then
            dataRpt[#dataRpt+1] = {}
            dataRpt[#dataRpt].val = ''
            if rpt then dataRpt[#dataRpt].idx = {1} end
            if dataRpt[#dataRpt].idx == nil then dataRpt[#dataRpt].idx = {} end
         end
         break
      elseif rpt then
         for j=1, #data do
            local tmpData = data
            local tmpHL7Loc = location
            tmpHL7Loc[i] = j
            
            local tmpData = IREproc.getHL7Data{hl7Location=table.concat(tmpHL7Loc, ',', i), required=T.required, data=tmpData}
            for k=1, #tmpData do
               if tmpData[k].idx == nil then tmpData[k].idx = {} end
               table.insert(tmpData[k].idx, 1, j)
               table.insert(dataRpt, tmpData[k])
            end
         end
         break
      else
         if i == #location then
            -- EMPTY DATA Decision
            if (IREproc.allowEmpty and T.required) or (data[location[i]]:nodeValue() ~= '' and data[location[i]]:nodeValue() ~= '""') then
               dataRpt[#dataRpt+1] = {}
               dataRpt[#dataRpt].val = data[location[i]]:nodeValue()
               if rpt then dataRpt[#dataRpt].idx = {1} end
               if dataRpt[#dataRpt].idx == nil then dataRpt[#dataRpt].idx = {} end
            end
         end
      end
      data = data[location[i]]
   end
   return dataRpt
end

function IREproc.getKeyName(ruleTable, dataGUID, elementKeyType)
   if not ruleTable.IREmsh.elementKeyType or ruleTable.IREmsh.elementKeyType == '' then
      ruleTable.IREmsh.elementKeyType = IRErs.elementKeyType.alias
      --error("IREmsh.elementKeyType has not been set!", 5)
   end
   if not elementKeyType then elementKeyType = ruleTable.IREmsh.elementKeyType end
   
   if elementKeyType == IRErs.elementKeyType.dataGUID then
      return dataGUID
   else
      if ruleTable.Xref[dataGUID][elementKeyType] == nil then
         error("elementKeyType is invalid!!", 4)
      end
      return ruleTable.Xref[dataGUID][elementKeyType]
   end
end
IRErs = {}
IRErs.conn = db.connect{api=db.SQLITE, name='Rule_Store.sqlite'}
IRErs.live = true
IRErs.validateDataGUIDonStore = false

IRErs.elementKeyType = {
   alias = "Alias",
   dataGUID = "Data_GUID",
   indexedDataGUID = "Special"}

IRErs.DataTypes = {}
IRErs.sampleData = {}
IRErs.sampleData.DataTypes = {
   {Name='Message', Description='HL7 Message', isArray=true},
   {Name='Segment', Description='HL7 Segment', isArray=true},
   {Name='SegmentGroup', Description='HL7 Segment Group', isArray=true},
   {Name='Composite', Description='HL7 Composite', isArray=true},
   {Name='FieldArray', Description='JSON or XML element with child nodes', isArray=true},
   {Name='Field', Description='Generic data field', isArray=false},
   {Name='String', Description='String value data field', isArray=false},
   {Name='Number', Description='Number value data field', isArray=false},
   {Name='DateTime', Description='DateTime data field', isArray=false},
   {Name='Double', Description='Double precision floating-point data field', isArray=false},
   {Name='CustomField', Description='Dynamicly generated field value', isArray=false},
   {Name='DBTable_Select', Description='Database table select', isArray=true},
   {Name='DBTable_Update', Description='Database table update only', isArray=true},
   {Name='DBTable_Insert', Description='Database table insert only', isArray=true},
   {Name='Database', Description='Generic Database', isArray=true},
   {Name='DBTable_InsertUpdate', Description='Database table insert or update', isArray=true}
}

function IRErs.getFlowDetails(T)
   if not T.srcRuleGUID and not T.dstRuleGUID then error("srcRuleGUID or dstRuleGUID required!", 2) end
   if not T.srcEndpointGUID then error("srcEndpointGUID required!", 2) end
   if not T.dstEndpointGUID then error("dstEndpointGUID required!", 2) end
   local results = nil
   
   local qry = "SELECT * FROM Flow WHERE"..
   " SrcEndpoint_GUID='"..T.srcEndpointGUID.."'"..
   " AND DstEndpoint_GUID='"..T.dstEndpointGUID.."'"
   if T.srcRuleGUID then
      qry = qry.." AND SourceRule_GUID='"..T.srcRuleGUID.."'"
   end
   if T.dstRuleGUID then
      qry = qry.." AND DestRule_GUID='"..T.dstRuleGUID.."'"
   end
   qry = qry.."ORDER BY TimeStamp DESC LIMIT 1;"
   local results = IRErs.conn:query{sql=qry, live=IRErs.live}
   if #results > 0 then
      return IRErs.sqlResultToArray(IRErs.conn:query{sql=qry, live=IRErs.live})[1]
   else
      return nil
   end
end

function IRErs.getMessageTypeDetails(T)
   if T.clientGUID == nil then error("clientGUID required!", 2) end
   if T.messageTypeGUID == nil then
      if T.formatType == nil then error("formatType or messageTypeGUID required!", 2) end
      if T.version == nil then error("version or messageTypeGUID required!", 2) end
      if T.messageCode == nil then error("messageCode or messageTypeGUID required!", 2) end
   end
   
   local qry = "SELECT ID, Client_GUID, Format_Type, Message_Version, Message_Code, "..
   "Message_Type_GUID, Rule_GUID, TimeStamp FROM Rules WHERE"
   if T.messageTypeGUID ~= nil then
      qry = qry.." Message_Type_GUID='"..T.messageTypeGUID.."'"
   else
      qry = qry.." Client_GUID='"..T.clientGUID.."' AND"..
      " Format_Type='"..T.formatType.."' AND"..
      " Message_Version='"..T.version.."' AND"..
      " Message_Code='"..T.messageCode.."'"
   end
   
   qry = qry.." ORDER BY TimeStamp DESC LIMIT 1;"
   --trace(qry)
   
   return IRErs.sqlResultToArray(IRErs.conn:query{sql=qry, live=IRErs.live})
end

function IRErs.storeRule(T, limit1)
   if T.data == nil then error("data required", 2 ) end
   
   if type(T.data) == 'string' then
      T.data = json.parse{data=T.data}
   end
   
   if T.data.IREmsh == nil then error("Incoming JSON incomplete!", 2) end
   if T.data.Xref == nil then error("Incoming JSON incomplete!", 2) end
   if T.data.DataTypes == nil then error("Incoming JSON incomplete!", 2) end
   
   local msgGUID = IRErs.checkAndInsertMessageTypeGUID(T.data.IREmsh)
   
   if msgGUID ~= T.data.IREmsh.Message_Type_GUID then
      iguana.logWarning("Message Type Duplicate! Message_GUID in rule will be updated to:"..msgGUID)
      --error("Message Type Duplicate! Message_GUID in rule will be updated to:"..msgGUID, 2)
      -- Update Rule.Xref.<Data_GUID>.Parent_GUID
      for k,v in pairs(T.data.Xref) do
         if v.Parent_GUID == T.data.IREmsh.Message_Type_GUID then v.Parent_GUID = msgGUID end
      end
      -- Update Message_Type_GUID to match the new home DB configuration
      T.data.IREmsh.Message_Type_GUID = msgGUID
   end
   
   local flow = nil
   if T.data.Flow ~= nil then
      flow = IRErs.getTableCopy(T.data.Flow)
      T.data.Flow = nil
   end
   
   -- Look for new Alias Types and insert as needed and update rule
   for k,v in pairs(T.data.DataTypes) do
      local ID = IRErs.checkAndInsertDataType{Name=k, insert=false}
      if not ID then
         v.Name = k
         v.insert = true
         ID = IRErs.checkAndInsertDataType(v)
         v.ID = ID
         v.insert = nil
         v.Name = nil
         v.New = nil
      end
      if not v.ID then v.ID = ID end
   end
   
   -- Look for new Data_GUID's and change as needed
   for k,v in pairs(T.data.Xref) do
      if v.New then
         if IRErs.validateDataGUIDonStore then
            --IRErs.getUniqueGUID(sqlTable, sqlColumn, existingGUID, useWildcard)
            local GUID = IRErs.getUniqueGUID('Rules', 'Rule', k, true)
            if GUID ~= k then
               -- Update any child Data_GUID's Parent_GUID to ensure proper nesting
               for x,y in pairs(T.data.Xref) do
                  if y.Parent_GUID == k then y.Parent_GUID = GUID end
               end
            end
            k = GUID
         end
         v.New = nil
      end
   end
   
   local oldRuleGUID = T.data.IREmsh.Rule_GUID
   local ruleGUID, ruleID = IRErs.insertRule{rule=T.data, limit1=limit1}
   
   if ruleGUID ~= oldRuleGUID then
      iguana.logWarning("Rule_GUID Duplicate! A new Rule_GUID has been generated.")
      --error("Rule_GUID Duplicate! A new Rule_GUID has been generated.", 2)
      T.data.IREmsh.Rule_GUID = ruleGUID
   end
   
   if flow and ruleID then
      if flow.SourceRule_GUID == oldRuleGUID then flow.SourceRule_GUID = ruleGUID end
      if flow.DestRule_GUID == oldRuleGUID then flow.DestRule_GUID = ruleGUID end
      local flowID = IRErs.checkAndInsertFlow(flow)
   end
   
   return T.data.IREmsh, ruleGUID, ruleID
end

function IRErs.duplicateRuleFields(T, limit1)
   if type(T.data) == 'string' then T.data = json.parse{data=T.data} end
   local srcRule = IRErs.getRuleAsTable{ruleGUID=T.data.DuplicateFields.SrcRule_GUID}
   local dstRule = IRErs.getRuleAsTable{ruleGUID=T.data.DuplicateFields.DstRule_GUID}
   
   if T.data.DuplicateFields.Location_Match and T.data.DuplicateFields.Location_Match ~= '' then
      T.data.DuplicateFields.Data_GUID = IRErs.getLocationMatchDataGUID(T.data.DuplicateFields.Location_Match, srcRule.Xref)
   end
   
   if type(T.data.DuplicateFields.Data_GUID) == 'string' then
      T.data.DuplicateFields.Data_GUID = {T.data.DuplicateFields.Data_GUID}
   end
   trace(T.data.DuplicateFields.Data_GUID)
   
   for i=1, #T.data.DuplicateFields.Data_GUID do
      local Data_GUID = T.data.DuplicateFields.Data_GUID[i]
      dstRule.Xref[Data_GUID] = IRErs.getTableCopy(srcRule.Xref[Data_GUID])
      if dstRule.Xref[Data_GUID].Parent_GUID == srcRule.IREmsh.Message_Type_GUID then
         dstRule.Xref[Data_GUID].Parent_GUID = dstRule.IREmsh.Message_Type_GUID
      end
      
      if T.data.DuplicateFields.Dup_Parents then
         local addGUIDs = IRErs.getParentGUIDs(Data_GUID, srcRule)
         for j=1, #addGUIDs do
            dstRule.Xref[addGUIDs[j]] = IRErs.getTableCopy(srcRule.Xref[addGUIDs[j]])
            if dstRule.Xref[addGUIDs[j]].Parent_GUID == srcRule.IREmsh.Message_Type_GUID then
               dstRule.Xref[addGUIDs[j]].Parent_GUID = dstRule.IREmsh.Message_Type_GUID
            end
         end
      end
      
      if T.data.DuplicateFields.Dup_Children then
         local addGUIDs = IRErs.getChildGUIDs(Data_GUID, srcRule)
         for j=1, #addGUIDs do
            dstRule.Xref[addGUIDs[j]] = IRErs.getTableCopy(srcRule.Xref[addGUIDs[j]])
         end
      end
   end
   
   dstRule.IREmsh.Rule_GUID = IRErs.getUniqueGUID('Rules', 'Rule_GUID')
   
   if T.data.DuplicateFields.Create_Flow then
      if not T.data.DuplicateFields.SrcEndpoint_GUID or T.data.DuplicateFields.SrcEndpoint_GUID == '' or 
         not T.data.DuplicateFields.DstEndpoint_GUID or T.data.DuplicateFields.DstEndpoint_GUID == '' then 
         error("No Bueno!!") 
      end
      
      dstRule.Flow = {SourceRule_GUID=T.data.DuplicateFields.SrcRule_GUID,
         DestRule_GUID=dstRule.IREmsh.Rule_GUID,
         SrcEndpointGUID=T.data.DuplicateFields.SrcEndpoint_GUID,
         DstEndpointGUID=T.data.DuplicateFields.DstEndpoint_GUID}
   end
   
   return IRErs.storeRule({data=dstRule}, limit1)
end

function IRErs.getChildGUIDs(GUID, rule)
   local GUIDs = {}
   
   for k,v in pairs(rule.Xref) do
      if v.Parent_GUID == GUID then
         GUIDs[#GUIDs+1] = k
         if rule.DataTypes[v.DataType].isArray then
            local addGUIDs = IRErs.getChildGUIDs(k, rule)
            for i=1, #addGUIDs do
               GUIDs[#GUIDs+1] = addGUIDs[i]
            end
         end
      end
   end
   return GUIDs
end

function IRErs.getParentGUIDs(GUID, rule)
   local GUIDs = {}
   local cnt = 1
   while rule.Xref[GUID].Parent_GUID ~= rule.IREmsh.Message_Type_GUID do
      if cnt > 16 then break end
      GUIDs[#GUIDs+1] = rule.Xref[GUID].Parent_GUID
      GUID = GUIDs[#GUIDs]
      cnt = cnt+1
   end
   return GUIDs
end

function IRErs.getLocationMatchDataGUID(loc, srcXref)
   local dataGUID = {}
   for k,v in pairs(srcXref) do
      if v.Data_Location == loc then
         dataGUID[#dataGUID+1] = k
         
      end
   end
   return dataGUID
end

function IRErs.copyRule(T, limit1)
   local updRule, removed, altCnt
   if T.data.RuleCopy.OLDsrcRuleGUID and T.data.RuleCopy.NEWsrcRuleGUID then
      local OLD = IRErs.getRuleAsTable{ruleGUID=T.data.RuleCopy.OLDsrcRuleGUID}
      local NEW = IRErs.getRuleAsTable{ruleGUID=T.data.RuleCopy.NEWsrcRuleGUID}
      local UPD = IRErs.getRuleAsTable{ruleGUID=T.data.RuleCopy.dstRuleGUID}
      
      updRule, removed, altCnt = IRErs.transposeDataGUID{old=OLD, new=NEW, upd=UPD}
      updRule.Flow = {SourceRule_GUID=T.data.RuleCopy.NEWsrcRuleGUID,
         DestRule_GUID=T.data.RuleCopy.dstRuleGUID,
         SrcEndpointGUID=T.data.RuleCopy.srcEndpoint,
         DstEndpointGUID=T.data.RuleCopy.dstEndpoint}
      
      if T.data.RuleCopy.Updated_Name and T.data.RuleCopy.Updated_Name ~= '' then
         updRule.IREmsh.Name = T.data.RuleCopy.Updated_Name end
      if T.data.RuleCopy.Updated_Message_Code and T.data.RuleCopy.Updated_Message_Code ~= '' then
         updRule.IREmsh.Message_Code = T.data.RuleCopy.Updated_Message_Code end
      if T.data.RuleCopy.Updated_Version and T.data.RuleCopy.Updated_Version ~= '' then
         updRule.IREmsh.Version = T.data.RuleCopy.Updated_Version end
      if T.data.RuleCopy.Updated_Format and T.data.RuleCopy.Updated_Format ~= '' then
         updRule.IREmsh.Format = T.data.RuleCopy.Updated_Format end
      
   elseif T.data.RuleCopy.OLDdstRuleGUID and T.data.RuleCopy.NEWdstRuleGUID then
      error("Not implemented!!")
   end
   
   updRule.IREmsh.Rule_GUID = IRErs.getUniqueGUID('Rules', 'Rule_GUID', updRule.IREmsh.Rule_GUID)
   
   if T.data.RuleCopy.OLDsrcRuleGUID and T.data.RuleCopy.NEWsrcRuleGUID then
      updRule.Flow.DestRule_GUID = updRule.IREmsh.Rule_GUID
   elseif T.data.RuleCopy.OLDdstRuleGUID and T.data.RuleCopy.NEWdstRuleGUID then
      updRule.Flow.SourceRule_GUID = updRule.IREmsh.Rule_GUID
   end
	----[[
   local rule, RuleGUID, RuleID = IRErs.storeRule({data=updRule}, limit1)

   if RuleID then
      iguana.logInfo("Inserted New Rule with ID:"..RuleID.."\r\nRule_GUID:"..RuleGUID.."\r\nRule:\r\n"..json.serialize{data=updRule})
      if #removed > 0 then iguana.logInfo("Removed GUIDs:\r\n"..table.concat(removed, '\r\n')) end
   end
   --]]
   return rule, RuleGUID, RuleID, removed
end

function IRErs.transposeDataGUID(T)
   local removed = {}
   local cnt = 0
   local xRef = IRErs.getTableCopy(T.upd.Xref)
   local xGUIDs = {}
   local locationTbl = IRErs.getLocationTable(T.new.Xref)
   
   for k,v in pairs(xRef) do
      if T.old.Xref[k] then
         -- remove repeating signal from Data_Location for proper ID of key
         local locKey = v.Data_Location:gsub(',<[-0-9]+>','')
         if locationTbl[locKey] then
            local newGUID = locationTbl[locKey].Data_GUID
            if T.upd.Xref[newGUID] then error("processed this Data_GUID a 2nd time") end
            T.upd.Xref[newGUID] = IRErs.getTableCopy(T.upd.Xref[k])
            T.upd.Xref[k] = nil
            xGUIDs[k] = newGUID
            
            cnt = cnt + 1
         else
            iguana.logWarning("IRErs.transpose - Unable to locate new GUID for:"..k)
            removed[#removed+1] = k
            T.upd.Xref[k] = nil
         end
         -- Process CustomField for possible Source Data_GUID
      elseif v.DataType == 'CustomField' then
         local loc = json.serialize{data=v.ValueDef, compact=true}
         for st in loc:gmatch('\"Data_GUID\":\"%w+\"') do
            local dlGUID = st:gsub('\"Data_GUID\":\"',''):gsub('\"','')
            local lcKey = T.old.Xref[dlGUID].ValueDef:gsub(',<[-0-9]+>','')
            if locationTbl[lcKey] then 
               loc = loc:gsub(dlGUID, locationTbl[lcKey].Data_GUID)
            else
               removed[#removed+1] = dlGUID
            end
         end
         T.upd.Xref[k].ValueDef = json.parse{data=loc}
      else
         iguana.logInfo("IRErs.transpose - Unable to locate GUID in OLD rule:"..k)
         --T.upd.Xref[k] = nil
      end
   end
   
   -- Check and update Parent_GUID's that reference the old GUID
   for k,v in pairs(T.upd.Xref) do
      if xGUIDs[v.Parent_GUID] then
         T.upd.Xref[k].Parent_GUID = xGUIDs[v.Parent_GUID]
      end
   end
   return T.upd, removed, cnt
end

function IRErs.getLocationTable(tbl)
   local nTbl = {}
   for k,v in pairs(tbl) do
      -- remove repeating signal from Data_Location for proper ID of key
      local key = v.Data_Location:gsub(',<[-0-9]+>', '')
      nTbl[key] = v
      nTbl[key]['Data_GUID'] = k
   end
   return nTbl
end

function IRErs.getEquivGUID(GUID, xRefA, xRefB)
   local nGUID
   for k,v in pairs(xRefB) do
      if xRefA[GUID].DataType == v.DataType and xRefA[GUID].Data_Location == v.Data_Location then
         nGUID = k
         break
      end
   end
   return nGUID
end

function IRErs.getRuleAsTable(T)
   if not T.ruleGUID then error("ruleGUID required!", 2) end
	local sql = "SELECT Rule FROM Rules WHERE Rule_GUID='"..T.ruleGUID.."';"
   local result = IRErs.conn:query{sql=sql, live=IRErs.live}
   if #result > 0 then
      local rule = json.parse{data=result[1].Rule:nodeValue()}
      if rule.AliasTypes then
         rule = IRErs.aliasTypeToDataType(rule)
      end
      return rule
   else
      return nil
   end
end

function IRErs.aliasTypeToDataType(r)
   r.DataTypes = IRErs.getTableCopy(r.AliasTypes)
   r.AliasTypes = nil
   for k,v in pairs(r.Xref) do
      r.Xref[k].DataType = v.Alias_Type_Name
      r.Xref[k].Alias_Type_Name = nil
   end
   return r
end

function IRErs.getTableCopy(T)
	local tab = {}
   for k,v in pairs(T) do
      tab[k] = v
   end
   return tab
end

function IRErs.insertRule(T)
   if T.rule == nil then error("rule required!", 2) end
   if T.limit1 == nil then T.limit1 = false end
   
   if type(T.rule) == 'string' then
      T.rule = json.parse{data=T.rule}
   end
   
   local ID = nil
   local sql = "SELECT ID, Rule_GUID FROM Rules WHERE Client_GUID='"..T.rule.IREmsh.Client_GUID.."'"
   
   if T.limit1 then
      sql = sql..
         " AND Format_Type='"..T.rule.IREmsh.Format.."'"..
         " AND Message_Version='"..T.rule.IREmsh.Version.."'"..
         " AND Message_Code='"..T.rule.IREmsh.Message_Code.."'"..
         " AND Message_Type_GUID='"..T.rule.IREmsh.Message_Type_GUID.."'"
   else
      sql = sql.." AND Rule_GUID='"..T.rule.IREmsh.Rule_GUID.."'"
   end
   sql = sql..";"
   
   local result = IRErs.conn:query{sql=sql, live=IRErs.live}
   
   if T.limit1 and #result > 0 then
      --error("more than one")
   else
      T.rule.IREmsh.Rule_GUID = IRErs.getUniqueGUID('Rules', 'Rule_GUID', T.rule.IREmsh.Rule_GUID)
      
      sql = "INSERT INTO Rules (Client_GUID, Format_Type, Message_Version, Message_Code, Message_Type_GUID, Rule_GUID, Rule)"..
         "VALUES ('"..T.rule.IREmsh.Client_GUID..
         "', '"..T.rule.IREmsh.Format..
         "', '"..T.rule.IREmsh.Version..
         "', '"..T.rule.IREmsh.Message_Code..
         "', '"..T.rule.IREmsh.Message_Type_GUID..
         "', '"..T.rule.IREmsh.Rule_GUID..
         "', '"..IRErs.escapeSQLspecials(json.serialize{data=T.rule,compact=true}).."');"
      
      IRErs.conn:begin{}
      IRErs.conn:execute{sql=sql, live=IRErs.live}
      result = IRErs.getLastInsertID()
      IRErs.conn:commit{}
      ID = result[1].ID:nodeValue()
   end
   
   return T.rule.IREmsh.Rule_GUID, ID
end

function IRErs.generateDataTypes(dtTable)
   local res = IRErs.conn:query{sql='SELECT * FROM AliasTypes;', live=IRErs.live}
   if #res < #IRErs.sampleData.DataTypes then
      --[[
      if aliasTable ~= nil then
         if #aliasTable == 0 then aliasTable = IRErs.convertPairsToArray{data=aliasTable, keyName='Name'} end
      end
      if aliasTable == nil or #aliasTable == 0 then
         aliasTable = IRErs.sampleData.DataTypes
         --trace(#aliasTable)
      end
      for i=1, #aliasTable do
         aliasTable[i].insert = true
         IRErs.checkAndInsertAliasType(aliasTable[i])
         aliasTable[i].insert = false
      end
      --]]
   else
      res = IRErs.sqlResultToArray(res)
      for i=1, #res do
         IRErs.DataTypes[res[i].Name] = {}
         for k,v in pairs(res[i]) do
            if k ~= 'Name' then
               if k == 'isArray' then
                  IRErs.DataTypes[res[i].Name][k] = IRErs.sqlBitToBool(tonumber(v))
               else
                  IRErs.DataTypes[res[i].Name][k] = v
               end
            end
         end
      end
   end
end

function IRErs.checkAndInsertDataType(T)
   if not T.Name then error("Name required!", 2) end
   --if T.insert and not T.isArray then error("isArray required!", 2) end
   --if T.insert and not T.Description then error("Description required!", 2) end
   --{Name='', isArray='', Description=''}
   local ID = nil
   local qry = "SELECT * FROM AliasTypes WHERE Name='"..T.Name.."';"
   local result = IRErs.conn:query{sql=qry}
   
   if #result > 0 then
      ID = result[1].ID:nodeValue()
      if T.Updated then
         qry = "UPDATE AliasTypes SET isArray="..IRErs.anyToBoolInt(T.isArray)..
            ", Description='"..T.Description.."' WHERE ID=;"
         IRErs.conn:begin{}
         IRErs.conn:execute{sql=qry, live=IRErs.live}
         IRErs.conn:commit{}
      end
   elseif T.insert then
      qry = "INSERT INTO AliasTypes (Name, isArray, Description) VALUES ("..
         "'"..T.Name.."', "..IRErs.anyToBoolInt(T.isArray)..",'"..T.Description.."');"
      IRErs.conn:begin{}
      IRErs.conn:execute{sql=qry, live=IRErs.live}
      result = IRErs.getLastInsertID()
      IRErs.conn:commit{}
      ID = result[1].ID:nodeValue()
   end
   
   if T.Updated or IRErs.DataTypes[T.Name] == nil then
      IRErs.DataTypes[T.Name] = {ID = ID, isArray = T.isArray, Description = T.Description}
   end
   return ID
end

function IRErs.checkAndInsertFlow(T)
   local ID = nil
   local qry = "SELECT ID FROM Rules WHERE Rule_GUID='"..T.SourceRule_GUID.."';"
   if #IRErs.conn:query{sql=qry, live=IRErs.live} == 0 then
      error("No matching Source Rule_GUID", 2)
   end
   
   qry = "SELECT ID FROM Flow WHERE"..
   " SourceRule_GUID='"..T.SourceRule_GUID.."'"..
   " AND DestRule_GUID='"..T.DestRule_GUID.."'"..
   " AND SrcEndpoint_GUID='"..T.SrcEndpointGUID.."'"..
   " AND DstEndpoint_GUID='"..T.DstEndpointGUID.."';"
   
   local result = IRErs.conn:query{sql=qry, live=IRErs.live}
   
   if #result > 0 then
      ID = result[1].ID:nodeValue()
   else
      qry = "INSERT INTO Flow (SourceRule_GUID, DestRule_GUID, SrcEndpoint_GUID, DstEndpoint_GUID)"..
      " VALUES ('"..T.SourceRule_GUID.."','"..T.DestRule_GUID.."','"..T.SrcEndpointGUID.."','"..T.DstEndpointGUID.."');"
      IRErs.conn:begin{}
      IRErs.conn:execute{sql=qry, live=IRErs.live}
      result = IRErs.getLastInsertID()
      IRErs.conn:commit{}
      ID = result[1].ID:nodeValue()
   end
   return ID
end

function IRErs.checkAndInsertMessageTypeGUID(T)
   if T.Format == nil then error("Format required!", 2) end
   if T.Version == nil then error("Version required!", 2) end
   if T.Message_Code == nil then error("Message_Code required!", 2) end
   
   local GUID = nil
   
   local sql = "SELECT Message_TYPE_GUID FROM MessageTypes WHERE Format_Type='"..T.Format..
      "' AND Message_Version='"..T.Version.."' AND Message_Code='"..T.Message_Code.."';"
   local result = IRErs.conn:query{sql=sql}
   
   if #result > 0 then
      GUID = result[1].Message_TYPE_GUID:nodeValue()
   else
      GUID = IRErs.getUniqueGUID('MessageTypes', 'Message_Type_GUID', T.Message_Type_GUID)
      sql = "INSERT INTO MessageTypes (Format_Type, Message_Version, Message_Code, Message_TYPE_GUID) VALUES ('"..
      T.Format.."','"..T.Version.."','"..T.Message_Code.."','"..GUID.."');"
      IRErs.conn:begin{}
      IRErs.conn:execute{sql=sql, live=IRErs.live}
      IRErs.conn:commit{}
   end
   
   return GUID
end

function IRErs.getUniqueGUID(sqlTable, sqlColumn, existingGUID, useWildcard)
   local GUID = nil
   local unique = false
   local cnt = 0
   
   while not unique do
      if GUID == nil and existingGUID ~= nil then
         GUID = existingGUID
      else
         GUID = util.guid(128)
      end
      if cnt > 16 then
         error("IRErs.getUniqueGUID - Unable to generate a unique GUID after "..cnt.." attempts!")
      end
      local qry = "SELECT ID FROM `"..sqlTable.."` WHERE "..sqlColumn
      if useWildcard then
         qry = qry.." LIKE '%"..GUID.."%';"
      else
         qry = qry.."='"..GUID.."';"
      end
      qry = qry.."';"
      local result = IRErs.conn:query{sql=qry}
      if #result < 1 then unique = true end
      cnt = cnt+1
   end
   
   return GUID
end

function IRErs.getUniqueXrefGUID(ruleXref, existingGUID)
   local GUID = nil
   local unique = false
   local cnt = 0
   
   while not unique do
      if cnt > 16 then
         error("IRErs.getUniqueXrefGUID - Unable to generate a unique GUID after "..cnt.." attempts!")
      end
      if GUID == nil and existingGUID ~= nil then
         GUID = existingGUID
      else
         GUID = util.guid(128)
      end
      if not ruleXref[GUID] then unique = true end
      cnt = cnt+1
   end
   return GUID
end

function IRErs.escapeSQLspecials(text)
	if text  == nil then return nil end
   text = text:gsub("'", "''")
   text = text:gsub("*", "\*")
   return text
end

function IRErs.getLastInsertID()
   local r = nil
   if IRErs.conn:info().api == 1001 then
      r = IRErs.conn:query{sql='SELECT LAST_INSERT_ID() AS ID', live=IRErs.live}
   elseif IRErs.conn:info().api == 1013 then
      r = IRErs.conn:execute{sql='SELECT last_insert_rowid() as ID;', live=IRErs.live}
   end
   return r
end

function IRErs.sqlBitToBool(value, invert)
   local val = false
   if value == '\000' or value == 0 or value == '0' or tostring(value):lower() == 'false' then 
      val = false
   else
      val = true
   end
   if invert == true then
      if val == false then
         val = true
      else
         val = false
      end
   end
   
   return val
end

function IRErs.anyToBoolInt(value, invert)
   local val = 0
   if type(value) == 'string' then
      if value:lower() == 'true' then val=1 end
   elseif type(value) == 'boolean' then
      if value == true then val=1 end
   elseif type(value) == 'number' then
      if value > 0 then val=1 end
   end
   
   if invert == true then
      if val == 0 then
         val = 1
      else
         val = 0
      end
   end
   
   return val
end

function IRErs.sqlResultToArray(T)
   local tab = {}
   for i=1, #T do
      tab[i] = {}
      for j=1, #T[i] do
         local val = T[i][j]:nodeValue()
         if val == '\000' or val == '\001' then
            val = IRErs.sqlBitToBool(val)
         end
         tab[i][T[i][j]:nodeName()] = val
      end
   end
   if #tab > 0 then return tab
   else return nil end
end

function IRErs.convertPairsToArray(T)
   --{data=aliasTable, keyName='Name'}
   local table = {}
   for k,v in pairs(T.data) do
      table[#table+1] = v
      table[#table][T.keyName] = k
   end
   return table
end
require 'amplefi'
require 'stringutil'
require 'urlcode'

-- The main function is the first function called from Iguana.
function main()
   -- Keep this channel running even in the event of error
   iguana.stopOnError(false)
   iguana.setTimeout(600)
   LogsForMOS()
end

function getMaxTimeStamp()
   local sql = "SELECT MAX(time_stamp) AS ts FROM ecompass.dbo.perf_log"
   local rec = amplefi.ExecuteMsSql(sql)
   return tostring(rec[1].ts:nodeValue())
end

function LogsForMOS()
   local maxts = getMaxTimeStamp()
   local sql = "SELECT time_stamp,service,server,payload "
   sql = sql.."FROM performance.timeblock "
   sql = sql.."WHERE (service LIKE 'content_%' "
   sql = sql.."OR service = 'auth_authenticate') "
   sql = sql.."AND service <> 'content_response_story_count' "
   sql = sql.."AND time_stamp > '"..maxts.."' "
   if iguana.isTest() then
      sql = sql.."AND payload LIKE '%hmk%' "
      sql = sql.."AND service = 'content_response_story' "
      sql = sql.."AND payload LIKE '%story_854239534%' "
   end
   sql = sql.."ORDER BY time_stamp"
   --sql = sql.."LIMIT 1000"
   local ret = amplefi.ExecuteMySql(sql)
   local pl = ''     -- payload
   local ts = ''     -- time_stamp
   local srvc = ''   -- service
   local srvr = ''   -- server
   local uip = ''    -- user ip address
   local etime = ''  -- execution time
   local user = ''   -- user
   local mthd = ''   -- method
   local kval = ''   -- key value
   local cid = ''    -- client_id
   local qs = ''     -- query_string
   local k = ''      -- key
   local kv = ''     -- kvs value
   local kvk = ''    -- kvs key
   local dv = ''     -- device type
   local qusr = ''   -- user from query_string 
   local qd = ''     -- json to be queued
   for i = 1, #ret do
      ts = ret[i].time_stamp:nodeValue()
      srvc = ret[i].service:nodeValue()
      srvr = ret[i].server:nodeValue()
      pl = ret[i].payload:nodeValue()
      dv = getDeviceType(pl)
      if pl:sub(1,1) == '{' then
         local js = json.parse{data=pl}
         uip = js.context.http_x_forwarded_for
         etime = js.execution_time
         user = js.user
         mthd = js.method
         if srvc == 'auth_update_profile' then
            qs = js.context.query_string.query_string.query_string
         elseif srvc == 'auth_authenticate' then
            qs = js.context.query_string.query_string
         else
            qs = js.context.query_string
         end
         local Args = {}
         trace(qs)
         urlcode.parseQuery(qs, Args)
         cid = ''
         qusr = ''
         if Args.client_id ~= nil then
            cid = Args.client_id
         end
         if Args.client ~= nil then
            cid = Args.client
         end
         if cid ~= '' then
            cid = cid:gsub('"','')
         end
         if Args.user ~= nil then
            qusr = Args.user
         end
         local t = ''
         if Args.token ~= nil then
            t = Args.token
         end
         trace(t)
         k = ''
         if Args.key ~= nil then
            k = Args.key
         end
         kv = ''
         trace(k)
         if k ~= '' then
            local k1 = ''
            if cid ~= '' then
               if cid:upper() == 'HMK' then
                  k1 = 'ui_highmark_ben_'..k
               else
                  k1 = 'ui_highmark_'..cid..'_'..k
               end
            end
            trace(k1)
            kval = getKvsRec(k1)
            --trace(tostring(kval[i].kvs_value:nodeValue()))
            kvk = tostring(kval[i].kvs_key:nodeValue())
            kv = tostring(kval[i].kvs_value:nodeValue())
            if kv ~= '' then
               local jkv = json.parse{data=kv}
               trace(jkv[1].storyName)
               --trace(#jskval[1].slides)
               kv = '{"storyName":"'..tostring(jkv[1].storyName)..'"}'
            else
               kv = '{"storyName":""}'
            end
         else
            kv = '{"storyName":""}'
         end
         if uip == nil then
            uip = ''
         end
         qd = '{"ts":"'..ts..'","srvc":"'..srvc..'","srvr":"'
         qd = qd..srvr..'","dv":"'..dv..'","etime":"'
         qd = qd..etime..'","user":"'..qusr..'","uip":"'..uip..'","cid":"'
         qd = qd..cid:gsub('"','')..'","k":"'..k..'","kvk":"'..kvk..'","kv":'
         qd = qd..kv..',"session_data":"'..t..'"}'
         trace(qd)
         queue.push{data=qd}
      end
   end
end

function getDeviceType(pl)
   local dv = 'Unknown Device'
   if pl:find('iPhone') ~= nil then
      if pl:find('iPhone') > 0 then
         dv = 'iPhone'
      end
   end
   if pl:find('iPad') ~= nil then
      if pl:find('iPad') > 0 then
         dv = 'iPad'
      end
   end
   if pl:find('Android') ~= nil then
      if pl:find('Android') > 0 then
         dv = 'Android'
      end
   end
   if pl:find('WOW64') ~= nil then
      if pl:find('WOW64') > 0 then
         dv = 'WOW64'
      end
   end
   if pl:find('Macintosh') ~= nil then
      if pl:find('Macintosh') > 0 then
         dv = 'Macintosh'
      end
   end
   if pl:find('Windows NT') ~= nil then
      if pl:find('Windows NT') > 0 then
         dv = 'Windows NT'
      end
   end
   return dv
end

function getKvsRec(k1)
   --local sql = "SELECT * FROM application.kvs WHERE kvs_key LIKE '%"..k.."'"
   local sql = "SELECT kvs_key,kvs_value FROM application.kvs WHERE kvs_key = '"..k1.."';"
   local ret = amplefi.ExecuteMySql(sql)
   return ret
end

      --[[
      if ret[i].payload:nodeValue() ~= '' then
         dt = '{"time_stamp":"'..ret[i].time_stamp..'","service":"'
         dt = dt..ret[i].service..'","server":"'..ret[i].server..'","'
         local sp =ret[i].payload:nodeValue()
         local js = json.parse{data=sp}
      end
      --]]

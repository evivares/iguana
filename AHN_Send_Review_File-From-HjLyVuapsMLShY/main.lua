require('amplefi')
-- The main function is the first function called from Iguana.
function main()
   iguana.setTimeout(600)
   SendDataToQueue()
end

function SendDataToQueue()
   local dt = os.date('*t')
   local dts = os.date('%Y')..'-'..os.date('%m')..'-'
   dts = dts..('0'..(dt.day-1)):reverse():sub(1,2):reverse()
   dts = os.date('%Y-%m-%d')
   if iguana.isTest() then
      dts = '2015-10-26'
   end
   local sql = "SELECT * FROM ahn.data_log WHERE LEFT(time_stamp,10) = '"..dts.."'"
   local rec = amplefi.ExecuteMySql(sql)
   local qd = ''  -- queue data
   local rpos = '' -- row position in the file
   local rfn = '/ahn_outbound/ahnerrlog'..os.date('%Y%m%d')..'.txt'
   local lfn = '/tmp/ahnerrlog.txt'
   if #rec > 0 then
      local fd = 'npi|flaggedby|flaggednotes|flaggedtime|reason\n'
      qd = '{"position":"start","fmode":"w","data":"'..fd:gsub('"',"'")..'","rpath":"'..rfn
      qd = qd..'","lpath":"'..lfn..'"}'
      --trace(qd)
      queue.push{data=qd}
      for i = 1, #rec do
         fd = rec[i].file_key..'|'..rec[i].user..'|'..rec[i].comment
         fd = fd..'|'..rec[i].time_stamp..'|'..rec[i].reason..'\n'
         if i == #rec then
            rpos = 'end'
         else
            rpos = 'middle'
         end
         qd = '{"position":"'..rpos..'","fmode":"a","data":"'..fd:gsub('"',"'")..'","rpath":"'..rfn
         qd = qd..'","lpath":"'..lfn..'"}'
         --trace(qd)
         queue.push{data=qd}
      end
   end
end

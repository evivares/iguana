
--send this the begin date mm/dd/yy
--and an end date mm/dd/yy and it will return the total number of days
function daysbetween(B,E)
   m1 = tonumber(B:sub(1,2))
   d1 = B:sub(4,5)
   y1 = B:sub(7,8)
   
   m2 = tonumber(E:sub(1,2))
   d2 = E:sub(4,5)
   y2 = E:sub(7,8)
   

   
   ytotal = y2 -y1
   trace(y1)
   if ytotal == 0 then
      trace('years are equal figure out months then days')
      mtotal = m2 -m1
      if mtotal == 0 then
         trace('same year same month figure days')
         totality = d2 -d1+1
      else
         trace('same year different months figure days')
         totality = totalMonths(B,E)
      end
   else
      trace('years are not equal so figure out the years first')
      y = totalYears(B,E)
      m = totalMonths(B,E)
      totality = y + m
   end
   trace(totality)
   return totality
end

function totalMonths(B,E)
   totalmonths = m2-m1-1
   trace('same year '..totalmonths..' months difference')
   --get how many days in a month then subtract number of day in the month
   --to get how many days in the first month
   trace(m1)
   daytotal = 0
   if m1 == 1 or m1 == 3 or m1 == 5 or m1 == 7
      or m1 == 8 or m1 == 10 or m1 == 12 then
      trace('here'..d1)
      daytotal = d1-1+31
      trace(daytotal)
   elseif m1 == 4 or m1 == 6 or m1 == 9 or m1 == 11 then
      trace('here')
      daytotal = d1-1 + 30
   elseif m1 ==2 then 
      if tostring(y1/4):find('%.') == nil then
            daytotal = d1-1 +29
            trace('adding 29 leap year')
         else
            daytotal = d1-1 +28
            trace('adding 28 non leap year')
         end
   end
      

   while m2 > m1+1 do  
      m2 = m2 - 1
      trace(m2) 
      if m2 == 1 or m2 == 3 or m2 == 5 or m2 == 7
         or m2 == 8 or m2 == 10 or m2 == 12 then
         daytotal = daytotal + 31
      elseif m2 == 4 or m2 == 6 or m2 == 9 or m2 == 11 then
         daytotal =daytotal +30
      elseif m2 ==2 then 
         --figure out if it is a leap year if so then add 29 days
         --if not then add 28.  I am dividing year by 4 if it is a
         --leap year then it should be a whole number without a .
         --if it is not a leap year it will have the decimal somewhere
         if tostring(y1/4):find('%.') == nil then
            daytotal = daytotal +29
            trace('adding 29 leap year')
         else
            daytotal = daytotal +28
            trace('adding 28 non leap year')
         end
      end
      --trace(daytotal)
   end
   
   daytotal = daytotal + d2
   trace(daytotal)
   return daytotal
end


function totalYears(B,E)
   totalyears = y2-y1-1
      trace(totalyears)
      y2 = tonumber(y2)
      y1 = tonumber(y1)
      yearTotal = 0
      while y2 > y1+1 do 
         y2 = y2 - 1
         if tostring(y1/4):find('%.') == nil then
            yearTotal = yearTotal + 366
            trace('adding 366 leap year')
         else
            yearTotal = yearTotal + 365
            trace('adding 365 non leap year')
         end
      end
      trace(yearTotal)
   return yearTotal
end

--send this function the month, day, year and number of days to add to it
function newdate(curmonth, curday, curyear, dayfix)
    trace(dayfix)
   newday = tonumber(curday)+dayfix
   trace(newday)
   newmonth = tonumber(curmonth)
   newyear = tonumber(curyear)
   if newday > 31 then
      trace(here)
      if newmonth == 1 or newmonth == 3 or newmonth == 5 or newmonth == 7
         or newmonth == 8 or newmonth == 10 or newmonth == 12 then
         trace('here')
         newday = newday - 31
         newmonth = newmonth + 1
      end
      if newmonth > 12 then
         trace('here')
         newmonth = newmonth - 12
         newyear = newyear + 1
      end
   end
   trace(newmonth..' ' ..newday)
   if newday > 30 then
      if newmonth == 4 or newmonth == 6 or newmonth == 9 or newmonth == 11 then
         newday = newday - 30
         newmonth = newmonth + 1
      end
   end
   if newday > 28 and newmonth == 2
      and newyear/4 ~= tonumber(os.date('%d',newyear/4)) then
      newday = newday - 28
      newmonth = newmonth + 1
   end
   if newday > 29 and newmonth == 2 then
      newday = newday - 29
      newmonth = newmonth + 1
   end
   newday = newday
   newmonth = newmonth --format('%02d',newmonth)
   newyear = newyear --format('%02d',newyear)
   return newmonth, newday, newyear
end


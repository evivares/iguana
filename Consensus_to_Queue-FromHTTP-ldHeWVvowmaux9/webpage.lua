local Webpage = {}

Webpage.text = [[   
<h1>
   UIC HL7 ADT Web Service
</h1>

<p>To use this web service, please supply the following parameters:</p>

<ul>
   <li>
      <b>message:</b> Please provide the HL7 ADT v2.3 message in this parameter.
   </li>
</ul>

<p>The following is a pre-formatted request that you can use to send a message.</p>

<p>
   Post message is this format: <a href='?message=MSH'>http://localhost:667/api?message=MSH</a>
</p>
]]

return Webpage


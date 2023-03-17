#!/usr/bin/env python
#_*_ codig: utf8 _*_
log_Path="./logs" # Change for Linux
data_base_connect="host=10.10.130.38 dbname=cdndb user=vodtransfers3 password=vod-2022" #use (main) (functions:extract_xml_data, Duration_Transform)
aws_profile='pythonapps' #Use (functions:Dowload_Logs, extract_xml_data)
Mail_To=['ingenieriavcmc@vcmedios.com.co', 'Gtecnicovcmc@vcmedios.com.co', 'cparada@vcmedios.com.co'] #Use (functions:SendMail)
Buckets={   #Use (functions:extract_xml_data)
    "11": ["aenla-in-toolbox", "A&E"],
    "21": ["aenla-in-toolbox", "History"],
    "31": ["aenla-in-toolbox", "Lifetime"],
    "41": ["aenla-in-toolbox", "History2"],
    "62": ["spe-in-toolbox", "SONY-AXN"],
    }
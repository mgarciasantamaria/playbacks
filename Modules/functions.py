#!/usr/bin/env python
#_*_ codig: utf8 _*_
import datetime, psycopg2, boto3, sys, traceback, smtplib
import xml.etree.ElementTree as ET
from email.message import EmailMessage
from Modules.constants import *

def SendMail(text, mail_subject):
    msg = EmailMessage()
    msg.set_content(text)
    msg['Subject'] = mail_subject
    msg['From'] = 'alarmas-aws@vcmedios.com.co'
    msg['To'] = Mail_To
    conexion = smtplib.SMTP(host='10.10.130.217', port=25)
    conexion.ehlo()
    conexion.send_message(msg)
    conexion.quit()
    return

# Funcion que permite recuperar una lista de datos de un xml alojado en un bucket de AWS S3
def extract_xml_data(contentid, DATE_LOG):
    bucket = Buckets[contentid[6:8]][0]
    Folder= Buckets[contentid[6:8]][1]
    aws_session=boto3.Session(profile_name=aws_profile)
    s3_resource=aws_session.resource('s3')
    Object_key = Folder+'/'+contentid+'/'+contentid+'.xml'
    try:
        xml_data=s3_resource.Bucket(bucket).Object(Object_key).get()['Body'].read().decode('utf-8')
        xml_root = ET.fromstring(xml_data)
        contentType = xml_root.find("contentType").text
        data=[
            xml_root.find("externalId").text, # contentid
            contentType, # contenttype
            xml_root.find('channel').text #channel
        ]
        for title in xml_root.iter('title'):
            data.append(title.text) #title
            break
        if contentType=="movie":
            data.append("na") #serietitle
        elif contentType=="episode":
            for serietitle in xml_root.iter("seriesTitle"):
                data.append(serietitle.text) #serietitle
                break
        data.append(xml_root.find("release").text) #releaseyear
        if contentType=='movie':
            data.append('na') #season
            data.append('na') #episode
        elif contentType=='episode':     
            data.append(xml_root.find("season").text) #season
            data.append(xml_root.find("episode").text) #episode
            
        for genre in xml_root.iter('genre'):
            data.append(genre.text) #genre
            break
        for rating in xml_root.iter('rating'):
            data.append(rating.text) #rating
            break
        data.append(xml_root.find("duration").text) #duration
        print(data)
        postgresql=psycopg2.connect(data_base_connect)
        curpsql=postgresql.cursor()
        SQL="INSERT INTO xmldata VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        DATA=(
            data[0], #contentid
            data[1], #contenttype
            data[2], #channel
            data[3], #title
            data[4], #serietitle
            data[5], #releaseyear
            data[6], #season
            data[7], #episode
            data[8], #genre
            data[9], #rating
            data[10] #duration
        )
        curpsql.execute(SQL,DATA)
        postgresql.commit()
        postgresql.close()
        return DATA
    except:
        error=sys.exc_info()[2]
        errorinfo=traceback.format_tb(error)[0]
        text_print=f"{errorinfo}\n{str(sys.exc_info()[1])}"
        print_log(text_print, DATE_LOG)
        return 'error'
    #print(xml_data)
    
# Funcion que permite transfomar le dato de duracion del contenido cuando este no cumple el formato HH:MM:SS
def Duration_Transform(contentid):
    postgresql=psycopg2.connect(data_base_connect)
    curpsql=postgresql.cursor()
    curpsql.execute(f"SELECT duration FROM xmldata WHERE contentid like '%{contentid}%'")
    content=curpsql.fetchone()
    duration=content[0].split(':')
    while len(duration)>3:
        duration.remove('')
    duration_transformed=duration[0]+':'+duration[1]+':'+duration[2]
    curpsql.execute(f"UPDATE xmldata SET duration='{duration_transformed}' WHERE contentid LIKE '%{contentid}%'")
    postgresql.commit()
    postgresql.close()
    
    return duration

def print_log(TEXT, DATE_LOG):
    log_file=open(f"{log_Path}/{DATE_LOG}_log.txt", "a")
    log_file.write(f"{str(datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S'))}\t{TEXT}\n")
    log_file.close()
#!/usr/bin/env python
#_*_ codig: utf8 _*_
import psycopg2, sys, traceback, json
#from dateutil.relativedelta import relativedelta
import datetime
from Modules.functions import *
from Modules.constants import *

if __name__ == '__main__':
	#-------Variables------------------------------------------------------------------------
	dict={} #-Diccionario para guardar datos para escribir en el log de eventos
	count_manifests_deleted=0 #-Contador de manifest eliminados
	count_playbacks_registered=0 #-Contador de playbacks registrdos
	list_ErrorXmlData=[] #-Lista de IDs en los que hubo error de extrancion de data
	list_OkXmlData=[] #-lista de IDs en los que se extrajo la data correctamente
	list_error_duration=[] #-Lista de error dato duracion del contenido en el xml
	try:
		date_now=datetime.datetime.now()
		date_log=str(datetime.datetime.strftime(date_now, "%Y-%m-%d"))
		date_run=str(datetime.datetime.strftime(date_now - datetime.timedelta(days=1), "%Y-%m-%d")) #-Recopila el dato fecha y hora de la maquina en el momento que se ejecuta 
		text_print=f"Init"
		print_log(text_print, date_log)
		postgresql=psycopg2.connect(data_base_connect) #-Se crea la clase postgresql para establecer conexion con la base de datos con la funcion connect
		curpsql=postgresql.cursor() #-Se crea el metodo curpsql con el cual se ejecutaran comandos en el contexto de la sesion postgresql
#-------Se usa el atributo execute del metodo curlpsql para ejecutar comando SQL con el cual se solicita a
#-------la base de datos todos los manifests que concidan con el dato de fecha y hora de la variable date_now
#-------en el campo datetime de la tabla new_manifests
		curpsql.execute(f"SELECT * FROM new_manifests WHERE datetime LIKE '%{date_run}%' ORDER BY datetime")
#-------Se usa el metodo fecthall para guardar en forma de lista los datos recopilados por el comando SQL que
#-------antecede en la variable list_new_manifests  
		list_new_manifest=curpsql.fetchall() #-Se agrega al diccionario la cantidad total de manifest encontrados
		dict["Manifest_Finded"]=str(len(list_new_manifest))
#-------Ciclo for que recore la lista de manifests. La variable manifest contiene los registros en forma de tupla
#-------optenido en cada ciclo		
		for manifest in list_new_manifest:
			manifest_Id=manifest[4] #-Variable que almacena el dato del campo manifestid del registro manifest
			manifest_segduration=manifest[8] #-Variable que almacena el dato de duracion del segmento del campo segduration del registo manifest
			contentid=manifest[2][2:] #-Variable que almacena el datos del campo contentid del registro manifest
			curpsql.execute(f"SELECT COUNT(*) FROM new_segmentos WHERE manifestid LIKE '%{manifest_Id}%'") #-Se solicita a la base de datos la cuenta de los registros en la tabla new_segmentos
			count_Segments=curpsql.fetchone() #-Variable que almacena la cuenta de segmentos registrados
#-----------Condicion segun value de la variable contador de segmentos.        
			if count_Segments != (0,):
#---------------Se solicita a la base de datos los registros en la tabla xmldata que coincidad con el valor de la 
#---------------variable contentid                   
				curpsql.execute(f"SELECT * FROM xmldata WHERE contentid LIKE '%{contentid}%'")
				xmldata=curpsql.fetchone() #-Variable que almacena los registros de la solicitud que antecede
#---------------Condicion segun value de la variable xmldata 
				if xmldata == None:
					xmldata=extract_xml_data(contentid, date_log) #-Se agrega a la lista el contenido que retorna la funcion extract_xml_data
#-------------------Condicion que valida la extraccion correcta de los datos del xml
					if xmldata[0]=='error':
						list_ErrorXmlData.append((manifest_Id, contentid)) #-Se agrega a la lista los datos con los que se genere el error
						dict["ErrorXmlData"]=list_ErrorXmlData #-Se agrega al diccionario la lista de IDs donde hubo error de extracion de data de los xml
						continue #-cumplida la condicion se interrumpe el ciclo actual y continua con el otro cliclo
					else:
						list_OkXmlData.append((contentid)) #-Se agrega a la lista el contentid con el cual se extrajo lada correctamente del xml 
						dict["OkXmlData"]=list_OkXmlData #-Se agrega al dicionario la lista de IDs que arrojaron error al extraer la data de los XMLs
						pass
				duration=xmldata[10].split(':') #-Variable que almacena el dato de duracion del registro xmldata
#---------------Agrega a la lista los datos donde se produjo error en el dato de duracion del contenido 
				if len(duration)>3:
					list_error_duration.append((xmldata[10], contentid, manifest_Id)) #-Agrega al diccionario la lista de datos del error anterior
					dict["DuartionXmlError"]=list_error_duration #-Agrega al diccionario la lista de datos del error anterior
#-------------------Se llama a la funcion Duration_Tranform para reprar el dato erroneo de duracion del contenido   
					duration=Duration_Transform(contentid)
				else:
					pass	
#---------------Variable que almacena el dato de tiempo en segundos como un entero a partir  del formato datetime con el uso
#---------------de la clase timedelta del modulo datetime y el metodo total_seconds
				duration_seconds=int(
					datetime.timedelta(
					hours=int(duration[0]),
					minutes=int(duration[1]),
					seconds=int(duration[2])
					).total_seconds()
				)	
#---------------Variable que almacena el dato de la cuenta de segmentos multiplicado por la duracion del segmento como un entero
				count_segments_seconds=int(count_Segments[0])*int(manifest_segduration)
#---------------Condicion de acuerdo al value de las variables duracion en segundos de clip de video y la duracion en segundo 
#---------------del calculo de la cantidad de segmentos              
				if count_segments_seconds>=duration_seconds:
					view=100 #-Variable que almacena el dato del calculo del porcentaje de view
				else:
					view=(count_segments_seconds*100)/duration_seconds #-Variable que almacena el dato del calculo del porcentaje de view
#---------------Comando SQL para insertar datos en la tabla playbacks
				SQL="INSERT INTO playbacks VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
#---------------Tupla con los datos a insertar en la tabla playbacks 
				playbacks_Data=(
					manifest[0],    			# 1 datetime
					manifest[5],    			# 2 country
					manifest[6],    			# 3 MSO
					manifest[7],    			# 4 device
					manifest[1][2:],    		# 5 client id
					manifest[2][2:],    		# 6 content id
					xmldata[1],  				# 7 content type
					xmldata[2],  				# 8 channel
					xmldata[3],  				# 9 title
					xmldata[4],  				#10 serie title
					xmldata[5],  				#11 release year
					xmldata[6],  				#12 season
					xmldata[7],  				#13 episode
					xmldata[8],  				#14 genre
					xmldata[9],  				#15 rating
					str(duration_seconds), 		#16 clipduration
					str(count_segments_seconds),#17 segduration
					str(round(view, 2)), 		#18 ClipView percentage
				)
#---------------Ejecucion del comando SQL para insertar datos en la tabla playbacks
				curpsql.execute(SQL, playbacks_Data)
				postgresql.commit()
#---------------Ejecucion del comando SQL para respaldar los registros de los segmentos ya procesados
				curpsql.execute(f"INSERT INTO old_new_segmentos SELECT * FROM new_segmentos WHERE manifestid LIKE '%{manifest_Id}%'")
				postgresql.commit()
#---------------Ejecucion del comando SQL para respaldar los registros de los manifests ya procesados
				curpsql.execute(f"INSERT INTO old_new_manifests SELECT * FROM new_manifests WHERE manifestid LIKE '%{manifest_Id}%'")
				postgresql.commit()
#---------------Ejecucion del comando SQL para eliminar los registros de segmentos ya procesados
				curpsql.execute(f"DELETE FROM new_segmentos WHERE manifestid LIKE '%{manifest_Id}%'")
				postgresql.commit()
#---------------Ejecucion del comando SQL para eliminar los registros de manifets ya procesados
				curpsql.execute(f"DELETE FROM new_manifests WHERE manifestid LIKE '%{manifest_Id}%' ")
				postgresql.commit() #-Guarda los cambios realizados en la base de datos
				text_print=f"{manifest}"
				print_log(text_print, date_log)
				count_playbacks_registered+=1 #-Suma de los playbacks registrados en la base de datos
#---------------Instruccion si no se cumple la condicion manifests_segduration direrente de 'None'
			else:
#---------------Ejecucion del comando SQL para eliminar el registro de la tabla new_manifest           
				curpsql.execute(f"DELETE FROM new_manifests WHERE manifestid LIKE '%{manifest_Id}%' ")
				postgresql.commit() #-Se guardan los cambios realizados en la base de datos 
				text_print=f"Deleted {manifest}"
				print_log(text_print, date_log)
				count_manifests_deleted+=1 #-Se aumenta el contador de manifests eliminados 
				dict["Manifets_Deleted"]=str(count_manifests_deleted) #-Se agrega el valor de la cuenta de manifests eliminados al diccionario de respuesta
		postgresql.close() #-Se cierra la conexion a la base de datos 
		manifest_registered=str(len(list_new_manifest)-count_manifests_deleted) #-Se establece la cantidad de manifests registrados 
		dict["Playbacks_Registered"]=str(count_playbacks_registered) #-Se agraga al diccionario la cantidad de playbacks registrados
		dict_str_json=json.dumps(dict, sort_keys=False, indent=8) #-Se da formato tipo json al diccionario
#-------Se crea el archivo log que guarda el resumen de la ejecucion
		SendMail(str(dict_str_json), "Playbacks execution summary") #-Uso de la funcion SendMail para enviar email con el resumen de la ejecucion
		text_print=dict_str_json
		print_log(text_print, date_log)
		print(dict_str_json) #-Se imprime el resumen de la ejecucion
	except:
		error=sys.exc_info()[2] #-------Captura del error que arroja el sistema
		errorinfo=traceback.format_tb(error)[0] #-Captura el detalle del error
		dict["Playbacks_Registered"]=str(count_playbacks_registered) #-Se guerda en el diccionario resumen de los playbacks registrados antes del error
		dict["Excute_Error"]=[errorinfo, str(sys.exc_info()[1])] #-Se agrega al diccionario detalle del error generado
		dict["Xmldata"]=xmldata #-Se agrega al diccionario el Ultimo dato extraido de la tabla xmldata
		dict_str_json=json.dumps(dict, sort_keys=False, indent=8) #-Se da formato tipo json al dicionario 
#-------Se crea el archivo log con el resumen de la ejecucion y el error
		SendMail(str(dict_str_json), "Playbacks execution summary") #-Se usa la funcion SendEmail para enviar email con el resumen de la ejecucion y el error
		text_print=dict_str_json
		print_log(text_print, date_log)
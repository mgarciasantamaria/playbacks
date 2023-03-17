Funcionamiento General playbacks.py
El script se ejecuta todos los dias a las 00:15 horas como un subproceso de la solucion DataAnalitics en el servidor vodtransfers3
Funcionamiento General playbacks.py

Este trae los datos de la tabla new_manifests correspondientes al día anterior a la ejecución. Para cada registro de manifests toma el dato manifestID y cuenta los segmentos asociados al mismo que se encuentran en la tabla new_segmentos. Luego toma del registro manifests el dato contentID y trae los datos de la tabla xmldata asociados al mismo. En caso de no encontrar registros en la tabla xmldata se procede a buscar el archivo xml en la nube para extraer los datos y registrarlos en la tabla xmldata. Si no encuentra el xml en la nube con el contentid asociado se registra en el resumen de ejecución el contentID y se asocia como un error. Este error no interrumpe la ejecución del script como tampoco realizara registro de datos en la tabla playbacks para el manifest actual.  una vez obtenidos los datos y sean validos estos se agrupan y se crea un registro en la tabla playbacks con la siguiente información:

datetime --> Fecha y hora de la solicitud del manifests por el cliente. Dato extraído del registro manifests
country --> País donde se realiza la solicitud. Dato extraído del registro manifests.
MSO --> Cableoperador al que pertenece el cliente que realiza la solicitud.  Dato extraído del registro manifests.
device --> Dispositivo en el que se realiza la solicitud. Dato extraído del registro manifests.
client id --> identificación del cliente que realiza la solicitud. Dato extraído del registro manifests.
content id  --> ID del contenido. Dato extraído del registro manifests.
content type  --> Tipo de contenido. Dato extraído del registro xml.
channel  --> Canal al que pertenece el contenido. Dato extraído del registro xml.
title  --> Titulo del contenido. Dato extraído del registro xml.
serie title  --> Titulo de la serie "Na" si no aplica. Dato extraído del registro xml.
release year  --> Año de creación del contenido. Dato extraído del registro xml.
season  --> Temporada de la serie. "Na" si no aplica. Dato extraído del registro xml.
episode --> Numero de episodio. "Na" si no aplica. Dato extraído del registro xml.
genre --> Genero del contenido. Dato extraído del registro xml.
rating  --> Ranting del contenido. Dato extraído del registro xml.
clipduration  --> duración del contenido en segundos. Dato extraído del registro xml.
segduration  --> duración en segundos de la cuenta de segmentos entregados. Dato extraído de la tabla new_segmentos.
ClipView percentage --> relación entre la duración de reproducción y la duración total del contenido en porcentaje. Calculo realizado en el script.

Una vez finalizado el análisis se escribe un archivo log con el resumen de la ejecución. El nombre de este tiene la fecha de ejecución y a su vez se envía un email con la misma información del log.

Si durante la ejecucion del script se genera un error que interrumpa el funcionamiento de este se enviara un email con el resumen de la ejecución en ese instante incluido el detalle del error generado. A continuación, la estructura del resumen en formato json.
{
    "Manifest_Finded" : "000",                              # Muestra la cantidad de manifests encontrados durante la ejecucion del script
    "Manifests_Deleted" : "000",                            # Muestra la cantidad de manifests elimnidados durante la ejecucion del script. 
   “DurationXmlError” : [
        (“duration_data”, “contentid”, “manifest_id”)
        …],                                                                               # Muestra una lista de cada Contenido en los cuales encontró un error en el dato de duración del contenido extraído del xml. Se debe buscar el xml asociado al contentid y verificar el dato de duración. 
    "Error_Xml_Data" : [
        ("ContentID", "ManifestID"),
        ...],                                                                                # Muestra una lista de tuplas que contiene el content ID y manifest ID de los registros de los cuales no se pudo extraer la data del xml alojado en la nube
    "OkXmlData" : [
        "ContentID", 
        ...],                                                                              # Muestra la lista de contenID a los cuales se extrajo correctamente la data del xml asociado y alojado en la nube
    "Playbacks_Registered" : "000",                         # Muestra la cantidad de playback registrados en la base de datos
    "Excecute_Error" : [
        "Errorinfo", 
        "detalle", 
        "Registro manifests con el cual se genero el error"],               # Muestra detalle del error ocurrido durante la ejecución como también el registro manifests con el que ocurrió el error 
    "Xmldata" : ("registro")                                                                           # Muestra los datos del registro Xml para el contentID con el que se generó el error
}
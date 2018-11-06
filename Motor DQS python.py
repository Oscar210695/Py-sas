#/**********************************************************************
#				DATA QUALITY SYSTEM ENGINE - BANCOMER
#				Management Solutions (2017)
#**********************************************************************/
#
#exec_environment_PRO = 1;
#dqs_sas_path=/DEVSAS/dev/code/DQS;
#dqs_work=WORK;
#dqs_libout=WORK;
#dqs_run_date=datetime();
#today=%eval(10000*%sysfunc(year(%sysfunc(date())))+100*%sysfunc(month(%sysfunc(date())))+%sysfunc(day(%sysfunc(date()))));
#dqs_ora_path="BPABD001";
#dqs_ora_schema=GORAPR;
#dqs_ora_user="DBSBLC";
#dqs_ora_pass="23456789";
#
#%INCLUDE "/DEVSAS/dev/code/DQS/DQS_03_CATALOGUES.sas" / LRECL=80000;
#
#%put DQS RUN - &SYSDATE.-&SYSTIME.;
import cx_Oracle
import datetime
import pandas as pd 
import numpy as np

connection = cx_Oracle.connect("System","tavoms","localhost")

cursor = connection.cursor()
exec_environment_PRO = 1;
scheduled_id_plan=0
engine_full_execution=0

#1 - Get the execution plan information and prepare the variables for the engine to run;

cursor.execute("""BEGIN
                  FOR c IN ( SELECT table_name FROM user_tables WHERE table_name LIKE 'WORK_%' )
                  LOOP
                    EXECUTE IMMEDIATE 'DROP TABLE ' || c.table_name;
                  END LOOP;
                END;""")

cursor.execute("""SELECT CD_PLANIFICACION, nu_ban_detalle 
               FROM TLC3340_CFGPLANIMOT
               WHERE NU_TIPO    =1
               AND TM_INICIO_EJEC IS NULL
               AND TM_FIN_EJEC IS NULL
               order BY FH_PLANIFICACION asc""")

values=cursor.fetchone()

if values is not None:
    scheduled_id_plan=values[0]
    engine_full_execution=values[1]

BALANCE_TABLE=0;
MAX_BATCH = 1;
BATCH_FILTER=1;
NEWDAY=0;
VAL_TABLE=0;
BATCH_COLUMNA=0;
HISTORICA=0;
ONLY_TABLE=0;

cursor.execute("""SELECT  
            		CASE 
            			WHEN CF.CD_FORMATO = 1 THEN ''||FL.NB_LOCALICAMPO||''
            			WHEN CF.CD_FORMATO = 2 THEN 'PUT(INPUT('||FL.NB_LOCALICAMPO||',YYMMDD8.),DATE9.)'
            			WHEN CF.CD_FORMATO = 3 THEN 'PUT('||FL.NB_LOCALICAMPO||',DATE9.)'
            			WHEN CF.CD_FORMATO = 4 THEN 'PUT(DATEPART('||FL.NB_LOCALICAMPO||'),DATE9.)'
            		END AS NEWDAY, 
            		CASE 
            			WHEN CF.CD_FORMATO = 1 THEN ''||FL.NB_LOCALICAMPO||''
            			WHEN CF.CD_FORMATO = 2 THEN 'PUT(INPUT('||FL.NB_LOCALICAMPO||',YYMMDD8.),DATE9.)'
            			WHEN CF.CD_FORMATO = 3 THEN 'PUT('||FL.NB_LOCALICAMPO||',DATE9.)'
            			WHEN CF.CD_FORMATO = 4 THEN 'PUT(DATEPART('||FL.NB_LOCALICAMPO||'),DATE9.)'
            		END AS BATCH_FILTER,
            		DT.NU_HISTORICA,
            		DB.NB_BASE_DATOS ||'.'|| DT.NB_TABLA AS VALIDATION_TABLE,
            		FL.NB_LOCALICAMPO AS BATCH,
            		DT.NB_TABLA AS ONLY_TABLE
            		FROM TLC3340_CFGPLANIMOT CES
            		INNER JOIN TLC3339_CFGPLANMOT CEP
            		ON CES.CD_PLANIFICACION = CEP.CD_EJECUCION
            		INNER JOIN TLC3333_DICCIOCAMPO F
            		ON F.NU_INFORME = CEP.NU_INFORME
            		INNER JOIN TLC3334_INFORME P
            		ON F.CD_CAMPO = P.NU_COLUMNA_LOTE
            		INNER JOIN TLC3319_CFGFORMATO CF
            		ON CF.CD_FORMATO = F.NU_FORMATO
            		INNER JOIN TLC3351_LOCFISCAMPO FPL
            		ON FPL.NU_CAMPO = F.CD_CAMPO AND FPL.FH_DESASIGNACION IS NULL
            		INNER JOIN TLC3348_LOCALICAMPO FL
            		ON FL.CD_LOCALICAMPO = FPL.NU_LOCFISCAMPO
            		INNER JOIN TLC3338_DICCIOTABLA DT
            		ON DT.CD_TABLA = FL.NU_TABLA
            		INNER JOIN TLC3337_DICCIOBD DB
            		ON DB.CD_BASE_DATOS = DT.NU_BASE_DATOS
            		WHERE NU_TIPO    =1
            			AND TM_INICIO_EJEC  IS NULL
            			AND TM_FIN_EJEC IS NULL
            		ORDER BY FH_PLANIFICACION ASC """)

values=cursor.fetchone()
print(values)
if values is not None:
    NEWDAY=values[0]
    BATCH_FILTER=values[1]
    HISTORICA=values[2]
    VAL_TABLE=values[3]
    BATCH_COLUMNA=values[4]
    ONLY_TABLE=values[5]
    
print (NEWDAY)
print (scheduled_id_plan)
print (engine_full_execution)
print (BATCH_FILTER)
print (VAL_TABLE)
print (BATCH_COLUMNA)
print (ONLY_TABLE)

I=0;
POPULATION_TABLE=0;
MAX_BATCH=0;
BATCH_TABLE=0;

cursor.execute("""SELECT MAX(NU_EJECUCION) + 1
               FROM TLC3340_CFGPLANIMOT
               order BY FH_PLANIFICACION asc""")

values=cursor.fetchone()
ID_VALIDATION = 0
timestamp = datetime.datetime.utcnow()

if values is not None:
    ID_VALIDATION=values[0]

cursor.execute("""BEGIN
                   UPDATE TLC3340_CFGPLANIMOT 
                   SET TM_INICIO_EJEC = :timestamp, NU_EJECUCION = :ID_VALIDATION
                   WHERE CD_PLANIFICACION = :scheduled_id_plan;
                   COMMIT;
               END;""",timestamp=timestamp,ID_VALIDATION=ID_VALIDATION,scheduled_id_plan=scheduled_id_plan)

cursor.execute("""CREATE GLOBAL TEMPORARY TABLE WORK_error_tracking_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  (error_id NUMBER, message CHAR(600)) ON COMMIT PRESERVE ROWS""")
cursor.execute("""CREATE GLOBAL TEMPORARY TABLE WORK_dq_results_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  (id_qc NUMBER, numerator NUMBER, denominator NUMBER, result NUMBER, comments CHAR(600),qc_Date TIMESTAMP, id_validation NUMBER, balance_impact NUMBER, qc_batch_id NUMBER, total_population NUMBER, total_balance NUMBER) ON COMMIT PRESERVE ROWS""")
cursor.execute("""CREATE GLOBAL TEMPORARY TABLE WORK_dq_results_account_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  (id_qc NUMBER, id_validation NUMBER, account CHAR(500), value CHAR(500), balance_impact NUMBER, batch_id NUMBER, total_population NUMBER, sub_system_id CHAR(50)) ON COMMIT PRESERVE ROWS""")
cursor.execute("""CREATE GLOBAL TEMPORARY TABLE WORK_type_field_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  (Libname CHAR(8), memname CHAR(32), name CHAR(32), type NUMBER) ON COMMIT PRESERVE ROWS""")
cursor.execute("""CREATE GLOBAL TEMPORARY TABLE WORK_checks_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  (id_Qc NUMBER, query1 VARCHAR(4000), query2 VARCHAR(4000), active_from NUMBER, active_until NUMBER, execution_batch NUMBER, executed NUMBER) ON COMMIT PRESERVE ROWS""")

cursor.execute("""CREATE UNIQUE INDEX vr_keys
                  ON WORK_dq_results_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  (id_Qc,id_validation)""")

cursor.execute("""CREATE UNIQUE INDEX vra_keys
                  ON WORK_dq_results_account_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  (id_Qc,id_validation)""")

cursor.execute("""SELECT *
               FROM TLC3337_DICCIOBD""")

values =cursor.fetchall();

sysname = []
sysengi = []
syspath = []
syssche = []
sysuser = []
syspass = []

if values is not None:
    for row in values:
        sysname.append(row[1])
        sysengi.append(row[2])
        syspath.append(row[3])
        syssche.append(row[7])
        sysuser.append(row[5])
        sysuser.append(row[6])

#LIBRARIes miss logic
#for i in range(num):
#    if sysname[i] is not None:
#        if sysengi[i] == 2: #SAS
##                x=sysname[i]
##                exec("%s=%s" % (x,str(syspath[i])))
#                GORAPR = syspath[i]
#                print (GORAPR)
        
HOY_VERDAD=0
VARIABLE=0
I=0
print (I)
        
if isinstance(VAL_TABLE, str):
    TABLA = VAL_TABLE.split(".")
    print(TABLA)
    if BATCH_COLUMNA == 'CD_MES':
        while (POPULATION_TABLE == 0) and (I > -100):
            print (I)
            HOY=datetime.datetime.utcnow() + datetime.timedelta(I)
            BATCH_TABLE= HOY.month
            MAX_BATCH=BATCH_TABLE
            print(MAX_BATCH)
            #change in count """ + str(BATCH_COLUMNA) + """ string in count?
            #change table in  from """ + str(TABLA[1]) + """ table doesn´t exist
            #put where  WHERE """ + str(NEWDAY) + """= """ + str(MAX_BATCH) when table exist  
            cursor.execute("""SELECT COUNT(*) 
                              FROM  TLC3340_CFGPLANIMOT""")
            values=cursor.fetchone()
            if values is not None:
                POPULATION_TABLE = values[0]
            I = I - 1
    elif (HISTORICA == 1) or (HISTORICA == 2):
    	while (POPULATION_TABLE == 0) and (I > -100):
    		print (I)
    		HOY_VERDAD = datetime.datetime.utcnow()
    		HOY = datetime.datetime.utcnow() + datetime.timedelta(I)
    		MAX_BATCH = HOY.strftime("%B")
    		BATCH_TABLE = HOY.month
    		print(MAX_BATCH)
    		#change in count """ + str(BATCH_COLUMNA) + """ string in count?
    		#change table in  from """ + str(TABLA[1]) + """ table doesn´t exist
    		#put where  WHERE """ + str(NEWDAY) + """= """ + str(MAX_BATCH) when table exist  
    		cursor.execute("""SELECT COUNT(*) 
    					  FROM  TLC3340_CFGPLANIMOT""")
    		values=cursor.fetchone()
    		if values is not None:
    			POPULATION_TABLE = values[0]
    		I = I - 1
    else:
        while (VARIABLE == 0) and (I > -100):
            #change table in  from """ + str(TABLA[1]) + """ table doesn´t exist
            #put where  WHERE """ + str(NEWDAY) + """= """ + str(MAX_BATCH) when table exist
            cursor.execute("""SELECT COUNT(*) 
                          FROM  TLC3340_CFGPLANIMOT""")
            values=cursor.fetchone()
            if values is not None:
                POPULATION_TABLE = values[0]
            print(HISTORICA)
            print(I)
            HOY_VERDAD = datetime.datetime.utcnow()
            HOY = datetime.datetime.utcnow() + datetime.timedelta(I)
            MAX_BATCH = HOY.strftime("%B")
            BATCH_TABLE = HOY.month
            print (MAX_BATCH)
            #change table in  from """ + str(TABLA[1]) + """ table doesn´t exist
            #put where  WHERE """ + str(NEWDAY) + """= """ + str(MAX_BATCH) when table exist 
            cursor.execute("""SELECT COUNT(*) 
                      FROM  TLC3340_CFGPLANIMOT""")
            values=cursor.fetchone()
            if values is not None:
                VARIABLE = values[0]
            I = I - 1
            
print (POPULATION_TABLE)
print (MAX_BATCH)
BATCH_FINAL = MAX_BATCH
print (BATCH_FINAL)
print (BATCH_TABLE)

if HISTORICA == 2: 
    #change table in  from """ + str(TABLA[1]) + """ table doesn´t exist
    #put where  WHERE """ + str(NEWDAY) + """= """ + str(MAX_BATCH) when table exist 
    cursor.execute("""CREATE GLOBAL TEMPORARY TABLE WORK_""" + str(ONLY_TABLE) + """
                   AS (SELECT *
                   FROM TLC3340_CFGPLANIMOT)    """)

cursor.execute("""CREATE TABLE WORK_field_table_system_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  AS SELECT 
                        f.CD_LOCALICAMPO AS LOCATION_ID,
            			f.NB_LOCALICAMPO AS FIELD_NAME,
            			t.CD_TABLA		 AS TABLE_ID,
            			t.NB_TABLA		 AS TABLE_NAME,
            			d.CD_BASE_DATOS	 AS SYSTEM_ID,
            			d.NB_BASE_DATOS	 AS SYSTEM_NAME,
			            f.NU_BAN_PK      AS FLAG_PK
		          FROM	    TLC3348_LOCALICAMPO f
    			  LEFT JOIN	TLC3338_DICCIOTABLA t on f.NU_TABLA = t.CD_TABLA
    			  LEFT JOIN	TLC3337_DICCIOBD    d on t.NU_BASE_DATOS = d.CD_BASE_DATOS
				  ORDER BY t.CD_TABLA""")
cursor.execute("""SELECT * FROM WORK_field_table_system_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION))
field_table_system  = []
field_table_system = cursor.fetchall()
pk_list = []
I = 0

if field_table_system is not None:    
    for row in field_table_system:
        if I == 0:
            pk_list.append("")
        elif row[6] == 1:
            pk_list.append(row[1])
        else:
            pk_list.append("")
        I = I + 1
        
df1 = pd.DataFrame(field_table_system)    
df1['PK_LIST'] = pk_list

df = pd.read_sql(sql="""SELECT LOCATION_ID, 
                               FIELD_NAME , 
                               TABLE_ID   , 
                               TABLE_NAME , 
                               SYSTEM_ID  , 
                               SYSTEM_NAME, 
                               FLAG_PK 
                         FROM WORK_field_table_system_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION) ,con=connection)
print(df)

cursor.execute("""CREATE TABLE WORK_table_system_"""+str(exec_environment_PRO)+"""_"""+str(ID_VALIDATION)+""" 
                  AS SELECT 
                        t.CD_TABLA		 AS TABLE_ID,
            			t.NB_TABLA		 AS TABLE_NAME,
            			d.CD_BASE_DATOS	 AS SYSTEM_ID,
            			d.NB_BASE_DATOS	 AS SYSTEM_NAME
		          FROM	    TLC3338_DICCIOTABLA t
    			  LEFT JOIN	TLC3337_DICCIOBD    d on t.NU_BASE_DATOS = d.CD_BASE_DATOS
				  ORDER BY t.CD_TABLA""")

cursor.close()


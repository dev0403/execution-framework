ENV_NM = "DEV"

BASE_DIR="/app/alliance/iics"
LOG_DIR=BASE_DIR + "/logs"
SQL_DIR=BASE_DIR + "/sql"
PARM_DIR=BASE_DIR + "/parmfiles"
SCRIPTS_DIR=BASE_DIR + "/scripts"
CONFIG_DIR=BASE_DIR +  "/configs"

BASE_DATA_DIR = "/app/alliance/data"
SRCFILES_DIR = BASE_DATA_DIR + "/srcfiles"
TGTFILES_DIR = BASE_DATA_DIR + "/tgtfiles"
DROPBOX_DIR = BASE_DATA_DIR + "/incoming"
OUTBOUND_DIR = BASE_DATA_DIR + "/outbound"
HISTORY_DIR = BASE_DATA_DIR + "/history"

#DB connection details
DB_ADT_CONN_NM="SQL_ADT_SEROFFR"

GPG_PASSPHRASE = b'QWxsaWFuY2U=' #"Alliance" -- encoded in utf-8 format base64.b64encode(password.encode(encoding='UTF-8'))
#gpg_cmd = ["gpg --yes --batch --output - --passphrase=",base64.b64decode(GPG_PASSPHRASE).decode(),GPG_CONN_FILE]
GPG_CONN_FILE= BASE_DIR + "/connections/connections.info.gpg"

#Sql query to pull info from JOB_FLOW
JOB_FLOW_QRY_GLOBAL = """SELECT JOB_NM, PARM_NM, PARM_VAL, STEP_ID, INSRT_TS
                FROM SQL_ADT_SEROFFR.JOB_FLOW WHERE JOB_NM='%s' AND STEP_NM='GLOBAL'"""

#Sql query to pull info from JOB_FLOW
JOB_FLOW_QRY = """SELECT JOB_NM, STEP_NM, PCS_TYPE, PCS_PATH AS PCS_NM, JOB_CTL.PCS_NM AS PCS_SCRPT_NM, PCS_DESC,PARM_SEQ, PARM_NM, PARM_VAL, STEP_ID
                FROM SQL_STG_SERVOFFER_DEV01.JOB_CTL
                LEFT JOIN SQL_STG_SERVOFFER_DEV01.PCS
                ON JOB_CTL.PCS_NM = PCS.PCS_NM
                WHERE JOB_NM='%s' ORDER BY STEP_ID,PARM_SEQ,JOB_CTL.PCS_NM"""

CHECKPOINT_QRY = """SELECT JOB_NM,STEP_ID,CAST(ODATE AS VARCHAR(29)) AS ODATE,STATUS
                FROM SQL_STG_SERVOFFER_DEV01.JOB_CHKPOINT
                WHERE JOB_NM='%s' AND ODATE=CONVERT(VARCHAR(40),'%s',112)
                """

DELCHECKPOINT_QRY = """DELETE FROM SQL_STG_SERVOFFER_DEV01.JOB_CHKPOINT
                WHERE JOB_NM='%s' AND ODATE=CONVERT(VARCHAR(40),'%s',112)
                """

LOGENTRY_QRY = """INSERT INTO SQL_STG_SERVOFFER_DEV01.JOB_CHKPOINT
                    SELECT '%s','%s',CONVERT(VARCHAR(40),'%s',112),'%s'
                """
DLTENTRY_QRY = """DELETE FROM SQL_STG_SERVOFFER_DEV01.JOB_CHKPOINT
                    WHERE JOB_NM = '%s' AND ODATE=CONVERT(VARCHAR(40),'%s',112)
                """

PCS_EXE_STRT_STA_QRY = """INSERT INTO SQL_STG_SERVOFFER_DEV01.PCS_EXC_STATUS
                (JOB_NM, STEP_ID, STEP_NM, PCS_NM, RUN_ID, RUN_DT, PCS_ST_TS,  STATUS)
                SELECT '%s','%d','%s','%s','%s',CONVERT(VARCHAR(40),'%s',112),CONVERT(VARCHAR(40),'%s',120),'%s'
                """
PCS_EXE_END_STA_QRY = """UPDATE SQL_STG_SERVOFFER_DEV01.PCS_EXC_STATUS
                SET PCS_END_TS = CONVERT(VARCHAR(40),'%s',120),STATUS='%s'
                WHERE JOB_NM='%s' AND STEP_ID='%s' AND RUN_DT=CONVERT(VARCHAR(40),'%s',112) AND RUN_ID='%s'
                """

JOB_EXE_STRT_STA_QRY = """INSERT INTO SQL_STG_SERVOFFER_DEV01.JOB_EXC_STATUS
                (JOB_NM, RUN_ID, RUN_DT, JOB_ST_TS,  STATUS)
                SELECT '%s','%s',CONVERT(VARCHAR(40),'%s',112),CONVERT(VARCHAR(40),'%s',120),'%s'
                """
JOB_EXE_END_STA_QRY = """UPDATE SQL_STG_SERVOFFER_DEV01.JOB_EXC_STATUS
                SET JOB_END_TS = CONVERT(VARCHAR(40),'%s',120),STATUS='%s'
                WHERE JOB_NM='%s' AND RUN_DT=CONVERT(VARCHAR(40),'%s',112) AND RUN_ID='%s'
                """

PROCESS_TYPE_MAP = {
            "SHELL" : "sh",
            "PYTHON" : "python3",
            "PYTHON2" : "python",
            "PYTHON3" : "python3",
            "JAVA" : "java",
            "SPARK" : "spark-submit",
            "SCALA" : "scala",
            "OTHER" : ""
        }

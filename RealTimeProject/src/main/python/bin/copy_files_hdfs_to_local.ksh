# Declare a variable to hold the unix script name
JOBNAME="copy_files_hdfs_to_local.ksh"

# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Define teh log file where logs will be generated
LOGFILE="/home/${USER}/RealTimeProject/src/main/python/logs/${JOBNAME}_${date}.log"

{ # <--- Start of the log file
echo "${JOBNAME} Started...: ${date}"
LOCAL_OUTPUT_PATH="/home/${USER}/RealTimeProject/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/fact

HDFS_OUTPUT_PATH=PrescPipeline/output
HDFS_CITY_DIR=${HDFS_OUTPUT_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_OUTPUT_PATH}/presc

# Copy teh City and Fact file from HDFS to Local
hdfs dfs -get -f ${HDFS_CITY_DIR}/* ${LOCAL_CITY_DIR}/
hdfs dfs -get -f ${HDFS_FACT_DIR}/* ${LOCAL_FACT_DIR}/
echo "${JOBNAME} is complete...: ${date}"
} > ${LOGFILE} 2>&1 # <--- End of the program and end of log

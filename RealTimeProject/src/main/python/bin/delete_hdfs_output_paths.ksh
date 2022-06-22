# Declare a variable to hold the unix script name
JOBNAME="delete_hdfs_output_paths.ksh"

# Declare a variablet o hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Define the log file where logs would be genereated
LOGFILE="/home/${USER}/RealTimeProject/src/main/python/logs/${JOBNAME}_${date}.log"

{ # <--- Start of the log file
echo "${JOBNAME} Started...: ${date}"

CITY_PATH=PrescPipeline/output/dimension_city
hdfs dfs -test -d $CITY_PATH
status=$?
if [ $status == 0 ]
then
    echo "The HDFS output directory $CITY_PATH is available. Proceed to delete."
    hdfs dfs -rm -r -f $CITY_PATH
    echo "The HDFS output directory $CITY_PATH is deleted before extraction."
fi

FACT_PATH=PrescPipeline/output/fact
hdfs dfs -test -d $FACT_PATH
status=$?
if [ $status == 0 ]
then
    echo "The HDFS output directory $FACT_PATH is available. Proceed to delete."
    hdfs dfs -rm -r -f $FACT_PATH
    echo "The HDFS output directory $FACT_PATH is deleted before extraction."
fi

echo "${JOBNAME} is complete...: ${date}"
} > ${LOGFILE} 2>&1 # <--- End of program and end of log


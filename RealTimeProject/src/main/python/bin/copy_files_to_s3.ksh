# Declare a variable to hold the unix script name
JOBNAME="copy_files_to_s3.ksh"

# Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d_%H:%M:%S')

# Define teh log file where logs will be generated
LOGFILE="/home/${USER}/RealTimeProject/src/main/python/logs/${JOBNAME}_${date}.log"

{ # <--- Start of the log file
echo "${JOBNAME} Started...: ${date}"

LOCAL_OUTPUT_PATH="/home/${USER}/RealTimeProject/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/fact

for file in ${LOCAL_CITY_DIR}/*.*
do
    aws s3 --profile myprofile cp ${file} "s3://prescpipelinetraining/dimension_city/${bucket_subdir_name}/"
    echo "City file ${file} is pushed to s3."
done

for file in ${LOCAL_FACT_DIR}/*.*
do
    aws s3 --profile myprofile cp ${file} "s3://prescpipelinetraining/dimension_city/${bucket_subdir_name}/"
    echo "Fact file ${file} is pushed to s3."
done

echo "${JOBNAME} is complete...: ${date}"
} > ${LOGFILE} 2>&1 # <--- End of program and end of log file

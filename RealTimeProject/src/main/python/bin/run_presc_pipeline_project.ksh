PROJ_FOLDER="/home/${USER}/RealTimeProject/src/main/python"

# Call the copy_to_hdfs wrapper
printf "Calling the copy_files_local_do_hdfs.ksh ..."
${PROJ_FOLDER}/bin/copy_files_local_to_hdfs.ksh
printf "Executing copy_files_local_to_hdfs.ksh is complete at `date +"%d/%m/%Y_%H:%M:%S"` \n\n"

# Call below wrapper to delete HDFS Paths
printf "Calling the delete_hdfs_output_paths.ksh ..."
${PROJ_FOLDER}/bin/delete_hdfs_output_paths.ksh
printf "Executing delete_hdfs_output_paths.ksh is complete at `date +"%d/%m/%Y_%H:%M:%S"` \n\n"

# Call below Spark job to extract Fact and City files
printf "Calling run_presc_pipeline.py ..."
spark3-submit --master yarn --num-executors 28 --jars ${PROJ_FOLDER}/lib/postgresql-42.3.5.jar ${PROJ_FOLDER}/bin/run_presc_pipeline.py
printf "Executing run_presc_pipeline.py is complete at `date +"%d/%m/%Y_%H:%M:%S"` \n\n"

# Call below script to copy files from HDFS to local
printf "Calling copy_files_hdfs_to_local.ksh ..."
${PROJ_FOLDER}/bin/copy_files_hdfs_to_local.ksh
printf "Executing copy_files_hdfs_to_local.ksh is complete at `date +"%d/%m/%Y_%H:%M:%S"` \n\n"

# Call below script to copy files to s3
printf "Calling copy_files_to_s3.ksh ..."
${PROJ_FOLDER}/bin/copy_files_to_s3.ksh
printf "Executing copy_files_to_s3.ksh is complete at `date +"%d/%m/%Y_%H:%M:%S"` \n\n"


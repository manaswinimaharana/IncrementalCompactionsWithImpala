#/bin/bash
set +x
export PYTHON_EGG_CACHE=./myeggs

envn=${1}
days_old=${2}
file_name=${3}

. setenv.sh

ticket =""
wlf_kinit ticket

#Variable declaration 
date_val=`date +%Y%m%d`
backup_dir=${hdfs_base_dir}/data/compactions/${filename}
impala_cmd="impala shell --quite --delimited --ssl -i ${impala_host} -ku ${user_id}${impala_realm} -q"
log_file=${edge_model_dir}/compaction/${filename}_${date_val}_logs_*.txt

hdfs dfs -test -d  ${backup_dir}/backup_${date_val}

if [ $? == 0 ]; then  
   echo "Cleaning up the backup directory"
   hdfs dfs -rm -r -skipTrash ${backupdir}/backup_${date_val}
fi

echo " Creating the backup directory"
hdfs dfs -mkdir -p ${backup_dir}/backup_${date_val}

if [ $? != 0]; then 
 echo "Hadoop Make directory Failed"
 kdestroy -c $ticket
 exit 1
fi

#****************Process Begins *******************
cat ./${file_name} | while read rec; do 
#Begin reading the input file 
db_name=`echo ${rec} | awk -F '|' '{print $1}' | awk -F '.' '{print $1}' | sed -e "s/{lane}/$envn/"`
tb_name=`echo  ${rec} | awk -F '|' '{print $1}' | awk -F '.' '{print $2}'`
is_partitioned=`${impala_cmd}  "set request_pool =${request_pool}; show create table ${db_name}.${tb_nme};" | grep 'PARTITION' | wc -l`
schema=`${impala_cmd}  "set request_pool =${request_pool}; show create table ${db_name}.${tb_nme};"`

if [ '${is_partitioned}' != '' ]; then 
if [  ${is_partitioned -eq 0]; then 
echo  " $db_name.${tb_name is not partitioned table and is currently being compacted "
location=`echo ${schema} | awk -F 'LOCATION' '{print $2}' | awk -F '|' 'print $1}' | tr "'" " "`
location_np=`echo ${location} | awk -F ' ' '{print $1}'`
echo "Location : "  ${location_np}

hdfs dfs -mkdir -p ${backup_dir}/backup_${date_val}/${db_name}.${tb_name}
hdfs dfs -cp ${location_np} ${backup_dir}/backup_${date_val}/${db.name}.${tb_name}
if [ $? != 0 ]; then 
echo " Hadoop Copy job failed between "${location_np} $backup_dir}/${db.name}.${tb.name}
kdestroy -c $ticket
exit 1
fi

# Fetch the count before compactions 
before_count=$(${impala_cmd}  " set request_pool = ${request_pool}; refresh ${db_name}.${tb_name}; select count(*) as cnt_before from ${db_name}.${tb_name};")
 if [$? -ne 0]; then 
 echo "Before  Count Step Failed. Job Failed!"
 fi 
 
 #Perform the compaction 
 echo "performing compaction"
 `${impala_cmd} "set request_pool =${request_pool}; set PARQUET_FILE_SIZE=128M; refresh ${db_name}.${tb_name}; create table if not exists ${db_name}.${tb_name}_comp like ${db_name}.${tb_name};insert overwrite ${db_name}.${tb_name} select * from ${db_name}.${tb_name}; insert overwrite ${dbname}.${tb_name} select * from ${db_name}.${tb.name}_comp;"`
 
 if [$? ne 0 ]; then 
 echo "Compaction step failed! Job Failed "
 exit 1
 fi
 
 after_count=$(${impala_cmd} "set request_pool =${request_pool}; refresh ${db_name}.${tb_name}; select count(*) as cnt_after from  ${db_name}.${tb_name};")
 
 if [$? -ne 0 ]; then 
 echo " Before count step failed! job failed! "
 exit 1
 fi 
 
 if [$? eq 0] && [ "${before_count} "-eq "${after_count}" ]; then 
 echo "****************************************************************"
 echo " Counts match! ${db_name}.${tb_name} is compacted successfully"
 echo "*****************************************************************"
 else 
  echo "****************************************************************"
 echo " Counts mismatch! ${db_name}.${tb_name} is compaction failed"
 echo "*****************************************************************"
 fi
 
 else 
 
 echo "${dbname}.${tb_name} is a partitioned table and is currently being compacted"
 location_tm=`echo ${schema} | awk -F 'LOCATION' '{print $2}' | awk -F '|' '{print $1}' | tr "'" " "`
 location=`echo ${location_tm}| awk -F ' ' '{print $1}'`
 partition_column_name=`${impala_cmd}  " set request_pool=${request_pool}; show create tabke ${db_name}.${tb_name};"  | grep -A1 " PARTITIONED BY ( " | tr -d "\n" | tr -d "|" | awk -F '(' '{print $2}' | awk -F ' ' '{print $1}'`
 partition_column_name="${partition_column_name%"${partition_column_name##*[![:space:]]}"}"
 partition_column_type=`${impala_cmd} "set request_pool=${request_pool}; show create table ${db_name}.${tb_name};" | grep -A1 "PARTITIONED BY ( " | tr -d "\n" | tr -d "|" | awk -F '(' '{print $2}' | awk -F ' ' '{print $2}'`
 echo "location:${location}"
 echo  "partition_column_name: ${partition_column_name}"
 echo "partition_column_type: ${partition_column_type}"
 
 hdfs dfs -mkdir -p ${backup_dir}/backup_${date_val}/${db_name}.{tb_name}
 
 if [ $? ! =0 ]; then 
 echo "Hadoop Make directory for Partitioned table Failed"
 kdestroy -c $ticket
 exit 1
 fi
 
 cmd1="hdfs dfs -cp "
 #cmd1="hadoop distcp -cp "
 val=$(cat ./${filename}_files.txt |  awk 'BEGIN { ORS="" } { print p$0;{=" "} END  {print "\n"}')
 echo " backing up the directory"
 
 bck_cmd="${cmd1}${val} ${backup_dir}/backup_${date_val}/${db_name}.${tb_name}"
 echo "${bck_cmd}"
 `${bck_cmd}`
 
 if  [ $? -ne 0 ];  then 
 echo  "Backup Command Failed ! Job Failed" 
 exit 1
 fi
 
 if [ "${partition_column_type,,}" == "string" ]; then 
 part_02=`cat ./${filename}_files.txt |  awk -F '=' {'print $2'} | awk 'BEGIN { ORS = "" }  {print "\x27"} { print p$0; p=",\x27"} END {print "\n"}'`
 part_01="${partition_column_name} IN ("
 part_03="')"
 partition_value=${part_01}${part_02}${part_03}
 ehco ${partition_value} 
 else 
 part_02=`cat ./${file_name}_files.txt | awk -F '=' {'print $2'} | awk 'BEGIN { ORS=""} {print ""} {print p$0; p=","} END {print "\n"}'`
 part_03=")"
 partition_value=${part_01}${part_02}${part_03}
 echo ${partition_value}
 fi
 
 before_count=$(${impala_cmd} "set request_pool=${request_pool}; ALTER TABLE ${db_name}.${tb_name} RECOVER PARTITIONS ; select count(*) as cnt_before from ${db_name}.{tb_name} where ${partition_value};")
  if [ $? -ne 0 ] ; then 
  echo "before count step failed Job failed "
  exit 1
  fi
  
  echo " the count before ?? " ${before_count}
  
  if [ "${tb_name}" == "agg_superset_alignment" ] ; then 
  `${impala_cmd}  "set request_pool=${request_pool}; 
  set PARQUET_FILE_SIZE=128 MB;
  alter table ${db_name}.${tb_name} RECOVER PARTITIONS; 
  drop table if exists ${db_name}.${tb_name}_comp;
  create table if not exists ${db_name}.${tb_name}_comp like ${db_name}.${tb_name};
  insert overwrite ${db_name}.${tb_name}_comp partition (${partition_column_name}) [noshuffle] select * from ${db_name}.$tb_name} where ${partition_value};"`
  else
    `${impala_cmd}  "set request_pool=${request_pool}; 
  set PARQUET_FILE_SIZE=128 MB;
  alter table ${db_name}.${tb_name} RECOVER PARTITIONS; 
  drop table if exists ${db_name}.${tb_name}_comp;
  create table if not exists ${db_name}.${tb_name}_comp like ${db_name}.${tb_name};
  insert overwrite ${db_name}.${tb_name}_comp partition (${partition_column_name})  select * from ${db_name}.$tb_name} where ${partition_value};"`
  fi 
  
  if [ $? -ne 0 ]; then 
  echo "Compaction step failed! job failed !"
  exit 1
  fi 
  
  after_count =$(${impala_cmd} "set request_pool = ${request_pool}; ALTER TABLE ${db_name}.${tb_name} RECOVER PARTITIONS ; select count(*) as cnt_after from ${db_name}.${tb_name} where ${partition_value};")
  
  if [ $? -ne 0 ]; then 
  echo "After count step failed! Job Failed"
  exit 1
  fi 
  
  echo "the count after >> " ${after_count}
  
  if [ $? --eq 0 ] && [ "${before_count}" -eq "${after_count}" ]; then 
  echo "*****************************************************************"
  echo "Counts match! ${db_name}.$[tb_name} is compacted successfully"
  echo "*****************************************************************"
  else
    echo "*****************************************************************"
  echo "Counts mismatch! ${db_name}.$[tb_name} is compacted successfully"
  echo "*****************************************************************"
  exit 1
  fi
  
  rm -r ./${file_name}_files.txt
  
  else
  echo "*************************************************************"
  echo "No new Partitions to compact for ${db_name}.${tb_name}"
  echo "*************************************************************"
  fi
  
  #end of partitioned value
  fi
  
  #end of initial test
  fi
  #end  of reading the input file 
  done 
  #*********** Process Ends **************************************************
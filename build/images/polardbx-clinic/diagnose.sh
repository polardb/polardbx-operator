#!/bin/bash

# Copyright 2021 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# example: sh diagnose.sh -w workspace_path -n namespace

work_dir="."
namespace=default
operator_ns=polardbx-operator-system
dir_name=polardbx_diagnose_$(date +'%Y%m%d%H%M%S')
parse_args(){
  while getopts "hw:n:m:" option; do
      case $option in
          n)
              namespace=$OPTARG
            ;;
          h)  
              echo "sh diagnose.sh -n namespace"
              ;;
          w)
              work_dir=$OPTARG
              ;;
          m)
              operator_ns=$OPTARG
              ;;
          *)
              echo "Unknown option $option"
              ;;
      esac
  done
}
create_dump_dir(){
  if [ -d $dir_name ]; then
    echo "$dir_name has been exist. failed!"
    exit 1
  fi
  mkdir $dir_name
  if [ $? == 1 ]; then
    echo "failed to create directory $dir_name"
    exit 1
  fi
}

common_dump(){
  current_ns=$1
  pod_name=$2
  logfilename=$3
  echo "current_ns:"$current_ns "pod_name: "$pod_name "logfilename:"$logfilename
  echo "ulimit -a" >> $logfilename
  kubectl -n $current_ns exec -it $pod_name -- bash -c "ulimit -a" >> $logfilename
  echo "sysctl -a" >> $logfilename
  kubectl -n $current_ns exec -it $pod_name -- bash -c "sysctl -a" >> $logfilename
  echo "free -h" >> $logfilename
  kubectl -n $current_ns exec -it $pod_name -- bash -c "free -h" >> $logfilename
  echo "ps -ef" >> $logfilename
  kubectl -n $current_ns exec -it $pod_name -- bash -c "ps -ef" >> $logfilename
  echo "vmstat" >> $logfilename
  kubectl -n $current_ns exec -it $pod_name -- bash -c "vmstat" >> $logfilename
}

dump_obj(){
  current_ns=$1
  res_type=$2
  label_selector=$3
  need_describe=$4
  need_log=$5
  log_type=$6
  keyword=$7
  echo "dump obj current_ns=$current_ns res_type=$res_type label_selector=$label_selector need_describe=$need_describe need_log=$need_log log_type=$log_type keyword=$keyword "
  mkdir -p $current_ns/$res_type
  base_dir=$current_ns/$res_type
  if [ -n "$label_selector" ]; then
    echo "select $label_selector"
    kubectl -n $current_ns get $res_type -l $label_selector -o wide >  $base_dir/"$log_type"list.txt
  else
    kubectl -n $current_ns get $res_type -o wide > $base_dir/"$log_type"list.txt
  fi
  if [ -n "$keyword" ]; then
    head -n 1 $base_dir/"$log_type"list.txt > $base_dir/"$log_type"list2.txt
    cat $base_dir/"$log_type"list.txt | grep "$keyword" >> $base_dir/"$log_type"list2.txt
    rm -f $base_dir/"$log_type"list.txt
    mv $base_dir/"$log_type"list2.txt $base_dir/"$log_type"list.txt
  fi
  cat $base_dir/"$log_type"list.txt | awk 'FNR!=1 {print $1}' | xargs -I {}  bash -c "kubectl -n $current_ns get $res_type {} -oyaml > $base_dir/{}.yaml"
  if [ "$need_describe" == "1" ]; then
    cat $base_dir/"$log_type"list.txt | awk 'FNR!=1 {print $1}' | xargs -I {}  bash -c "kubectl -n $current_ns describe $res_type {}  > $base_dir/{}_describe.txt"
  fi
  if [ "$need_log" == "1" ]; then
    cat $base_dir/"$log_type"list.txt | awk 'FNR!=1 {print $1}' > $base_dir/"$log_type"podlist.txt
    lines=""
    while read line1
    do
      lines="$lines $line1"
    done < $base_dir/"$log_type"podlist.txt
    
    for line in $lines
    do
      echo $line
      containers=$(kubectl get pod "$line" -n "$current_ns" -o jsonpath='{.spec.containers[*].name}')
      for container in $containers
      do
          kubectl -n "$current_ns" logs "$line"  "$container" --since=24h > $base_dir/"$line"_"$container".log
      done
      if [ "$log_type" == "cn" ]; then
#        echo "collect /home/drds-server/logs/tddl/ logs ... for "$line
        kubectl -n "$current_ns" exec -it "$line" -- bash -c "cd /home/admin/drds-server/logs/tddl && tar zcvf tddllog.tar.gz *.log"
        kubectl -n "$current_ns" cp "$line":/home/admin/drds-server/logs/tddl/tddllog.tar.gz $base_dir/"$line"-tddlog.tar.gz
        kubectl -n "$current_ns" exec -it "$line" -- rm -f /home/admin/drds-server/logs/tddl/tddllog.tar.gz
        if ! [ -f cn.sql ]; then
          echo "
                select 'select version()';
                select version();
                select 'show storage';
                show storage \G;
                select 'show mpp';
                show mpp \G;
                select 'show full processlist';
                show full processlist;
                select 'show variables';
                show variables;
                " > cn.sql
        fi
        echo "exec cn.sql in "$line
        kubectl -n "$current_ns" cp cn.sql "$line":cn.sql
        kubectl -n "$current_ns" exec -it "$line" -- bash -c "myc -e 'source cn.sql'" >  $base_dir/"$line"-cmdresponse.log
        common_dump "$current_ns" "$line" $base_dir/"$line"-cmdresponse.log
      fi
      if [ "$log_type" == "dn" ]; then
            echo "collect  /data/mysql/log/alert.log  /data/mysql/conf ... for "$line
            kubectl -n "$current_ns" exec -it $line -- bash -c "cd /data/mysql/log/ && tar zcvf mysql.tar.gz alert.log /data/mysql/conf/*"
            kubectl -n "$current_ns" cp "$line":/data/mysql/log/mysql.tar.gz $base_dir/"$line"-mysql.tar.gz
            kubectl -n "$current_ns" exec -it $line -- rm -f /data/mysql/log/mysql.tar.gz
            echo "exec mysql -V in "$line
            echo "mysql -V" >> $base_dir/"$line"-cmdresponse.log
            kubectl -n "$current_ns" exec -it $line -- mysqld -V >> $base_dir/"$line"-cmdresponse.log
            if ! [ -f dn.sql ]; then
              echo "
                   select version();
                   select 'show processlist';
                   show processlist;
                   select 'show variables';
                   show variables;
                   select 'alisql_cluster_global';
                   select * from information_schema.alisql_cluster_global \G;
                   select 'alisql_cluster_local';
                   select * from information_schema.alisql_cluster_local \G;
                   select 'alisql_cluster_health';
                   select * from information_schema.alisql_cluster_health \G;
                   select 'show consensus logs';
                   show consensus logs \G;
                   select 'show slave status';
                   show slave status \G;
                   " > dn.sql
            fi
            echo "exec dn.sql in ""$line"
            kubectl -n "$current_ns" cp dn.sql "$line":dn.sql
            kubectl -n "$current_ns" exec -it "$line" -- myc -e 'source dn.sql' >> $base_dir/"$line"-cmdresponse.log
            common_dump "$current_ns" "$line" $base_dir/"$line"-cmdresponse.log
      fi
      if [ "$log_type" == "cdc" ]; then
        echo "collect  /home/admin/logs/polardbx-binlog ... for "$line
        kubectl -n "$current_ns" exec -it $line -- bash -c "cd /home/admin/logs/polardbx-binlog && tar zcvf cdc.tar.gz Daemon/*.log Dumper-1/*.log"
        kubectl -n "$current_ns" cp "$line":/home/admin/logs/polardbx-binlog/cdc.tar.gz $base_dir/"$line"-cdc.tar.gz
        kubectl -n "$current_ns" rm -f "$line":/home/admin/logs/polardbx-binlog/cdc.tar.gz
        common_dump "$current_ns" "$line" $base_dir/"$line"-cmdresponse.log
      fi
    done
  fi
  #try clean dir
  if [ -z "$label_selector" ]; then
    yamlfile_count=`ls $current_ns/$res_type/*.yaml | wc -l`
    if [ "$yamlfile_count" -eq 0 ]; then
      echo "delete dir $current_ns/$res_type"
      rm -fR $current_ns/$res_type
    fi
  fi
  
}


desensitize(){
  sed -i -E 's/(password: ).*/\1******/' $operator_ns/configmap/*.yaml
  sed -i -E 's/(minioAccessKey: ).*/\1******/' $operator_ns/configmap/*.yaml
  sed -i -E 's/(minioSecretKey: ).*/\1******/' $operator_ns/configmap/*.yaml
  sed -i -E 's/(accessKey: ).*/\1******/' $operator_ns/configmap/*.yaml
  sed -i -E 's/(accessSecret: ).*/\1******/' $operator_ns/configmap/*.yaml
  sed -i -E 's/(oss_access_key: ).*/\1******/' $operator_ns/configmap/*.yaml
  sed -i -E 's/(oss_access_secret: ).*/\1******/' $operator_ns/configmap/*.yaml
}


main(){
  res_types=("pxc" "xstore" "pxcblog" "pxb" "pbs" "pxcknobs" "pxlc" "pxm" "pxp" "pxpt" "st" "xsblog" "xsbackup" "xf" "deployment" "node")
  for res_type in "${res_types[@]}"
  do
      dump_obj $namespace $res_type &
  done
  wait
  dump_obj "$namespace" pod "polardbx/role=cn" 1 1 cn &
  dump_obj "$namespace" pod "xstore/node-role" 1 1 dn &
  dump_obj "$namespace" pod "polardbx/role=cdc" 1 1 cdc &
  dump_obj "$namespace" configmap "polardbx/name" && dump_obj "$namespace" configmap "xstore/name" &
  wait
  
  
  res_types_for_operator=("deployment" "ds")
  for res_type_for_operator in "${res_types_for_operator[@]}"
  do
      dump_obj $operator_ns $res_type_for_operator  &
  done
  dump_obj $operator_ns configmap "" 0 0 "" polardbx &
  dump_obj $operator_ns pod "" 1 1 "" &
  wait
  kubectl get nodes -o custom-columns=NAME:.metadata.name,CPU:.status.capacity.cpu,MEMORY:.status.capacity.memory,ALLOCATABLE_CPU:.status.allocatable.cpu,ALLOCATABLE_MEMORY:.status.allocatable.memory > noderesourcelist.txt
  desensitize
}



parse_args "$@"
echo "namespace : $namespace , work_dir : $work_dir"
cd $work_dir
#create dump directory
create_dump_dir
# switch dir to created directory
cd $dir_name/
main


cd -
tar zcvf polardbx-diagnose.tar.gz $dir_name

touch finished
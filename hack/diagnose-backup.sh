#!/usr/bin/env bash
set -euo pipefail

# diagnose-backup.sh
# 用途：快速诊断 PolarDBXBackup 及其子 XStoreBackup 的卡顿原因，输出关键信息（phase/message/owner Job/事件）。
# 使用：
#   hack/diagnose-backup.sh <namespace> <backup-name>
# 依赖：kubectl、jq（可选）

ns="${1:-}"
name="${2:-}"
if [[ -z "$ns" || -z "$name" ]]; then
  echo "用法: $0 <namespace> <backup-name>" >&2
  exit 1
fi

echo "== 顶层 PolarDBXBackup: ${ns}/${name} =="
kubectl get polardbxbackups.polardbx.aliyun.com -n "$ns" "$name" -o json | \
  jq '{name:.metadata.name, ns:.metadata.namespace, phase:.status.phase, message:.status.message, startTime:.status.startTime, completionTime:.status.completionTime}' 2>/dev/null || \
  kubectl get polardbxbackups.polardbx.aliyun.com -n "$ns" "$name" -o yaml

echo
echo "== 顶层 Events (describe 摘要) =="
kubectl describe polardbxbackups.polardbx.aliyun.com -n "$ns" "$name" | awk '/Events:/,/^$/'

echo
echo "== 关联的子 XStoreBackup（按标签 polardbx/top-backup=${name}）=="
kubectl get xstorebackups.polardbx.aliyun.com -n "$ns" -l "polardbx/top-backup=${name}" -o json | \
  jq -r '.items[] | "- " + .metadata.name + "\tphase=" + ( .status.phase // "" ) + "\tmessage=" + ( .status.message // "" )' 2>/dev/null || true

echo
echo "== 对每个子 XStoreBackup 做详细检查 =="
for xsb in $(kubectl get xstorebackups.polardbx.aliyun.com -n "$ns" -l "polardbx/top-backup=${name}" -o name | awk -F/ '{print $2}'); do
  echo "--- 子备份: ${xsb} ---"
  kubectl get xstorebackups.polardbx.aliyun.com -n "$ns" "$xsb" -o json | \
    jq '{name:.metadata.name, phase:.status.phase, message:.status.message, ownerRefs:.metadata.ownerReferences}' 2>/dev/null || \
    kubectl get xstorebackups.polardbx.aliyun.com -n "$ns" "$xsb" -o yaml

  echo "[describe Events]"
  kubectl describe xstorebackups.polardbx.aliyun.com -n "$ns" "$xsb" | awk '/Events:/,/^$/'

  # 寻找 Owner Job（可能通过 ownerReferences 或者命名约定），尝试抓取 Job 与 Pod 事件
  jobName=""
  jobName=$(kubectl get xstorebackups.polardbx.aliyun.com -n "$ns" "$xsb" -o jsonpath='{.metadata.ownerReferences[?(@.kind=="Job")].name}' 2>/dev/null || true)
  if [[ -z "$jobName" ]]; then
    # 常见命名：<xsb-name>-job 或包含 xsb 名称的 job，尝试模糊匹配
    jobName=$(kubectl get jobs -n "$ns" -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.ownerReferences[*].name}{"\n"}{end}' | awk -v xsb="$xsb" '$0 ~ xsb {print $1; exit}')
  fi

  if [[ -n "$jobName" ]]; then
    echo "[Owner Job] $jobName"
    kubectl get job -n "$ns" "$jobName" -o yaml | awk 'NR<=80{print} NR==81{print "... (truncated)"; exit}'
    echo "[Job Events]"
    kubectl describe job -n "$ns" "$jobName" | awk '/Events:/,/^$/'

    echo "[Pods of Job]"
    pods=$(kubectl get pods -n "$ns" -l job-name="$jobName" -o name)
    for p in $pods; do
      pn=${p#pod/}
      echo "  * Pod: $pn"
      kubectl get pod -n "$ns" "$pn" -o wide | sed '1,1!d;2,2!d'
      echo "  [Pod Events]"
      kubectl describe pod -n "$ns" "$pn" | awk '/Events:/,/^$/'
      echo "  [Recent Logs]"
      # 尝试抓取主容器日志
      kubectl logs -n "$ns" "$pn" --tail=100 || true
    done
  else
    echo "[Owner Job] 未找到（可能尚未创建或控制器未写入 ownerReferences）"
  fi
  echo
done

echo "== 完成 =="

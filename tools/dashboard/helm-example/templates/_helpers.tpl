{{/*
Expand the name of the chart.
*/}}
{{- define "polardbx.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "polardbx.dashboard.backend.fullname" -}}
{{- if .Values.backend.fullnameOverride }}
{{- .Values.backend.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-backend" (include "polardbx.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "polardbx.dashboard.backend.labels" -}}
helm.sh/chart: {{ include "polardbx.chart" . }}
{{ include "polardbx.dashboard.backend.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "polardbx.dashboard.backend.selectorLabels" -}}
app.kubernetes.io/name: {{ include "polardbx.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: backend
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "polardbx.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Service account name
*/}}
{{- define "polardbx.dashboard.backend.serviceAccountName" -}}
{{- $sa := .Values.serviceAccount | default dict -}}
{{- $create := (get $sa "create") | default false -}}
{{- $name := (get $sa "name") | default "" -}}
{{- if $create -}}
{{- default (include "polardbx.dashboard.backend.fullname" .) $name -}}
{{- else -}}
{{- default "default" $name -}}
{{- end -}}
{{- end }}


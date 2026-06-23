{{/* Expand the name of the chart. */}}
{{- define "spangen.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name. */}}
{{- define "spangen.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "spangen.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "spangen.labels" -}}
helm.sh/chart: {{ include "spangen.chart" . }}
{{ include "spangen.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "spangen.selectorLabels" -}}
app.kubernetes.io/name: {{ include "spangen.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Name of the Secret holding the ClickHouse password (created or existing). */}}
{{- define "spangen.chSecretName" -}}
{{- if .Values.clickhouse.existingSecret -}}
{{- .Values.clickhouse.existingSecret -}}
{{- else -}}
{{- printf "%s-ch" (include "spangen.fullname" .) -}}
{{- end -}}
{{- end -}}

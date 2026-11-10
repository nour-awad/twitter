{{- define "twitter.envKey" -}}
{{- $n := . | replace "-service" "" | replace "-orchestrator" "" | replace "api-gateway" "gateway" | replace "-" "_" | upper -}}
{{- $n -}}
{{- end -}}

{{- define "twitter.labels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/part-of: twitter
app: {{ .name }}
version: v1
{{- end -}}

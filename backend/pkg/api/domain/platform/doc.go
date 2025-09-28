package platform

// Package platform 提供平台横切能力的统一入口（监控、Grafana、日志、系统、Pod）。
//
// 领域边界（BFF）：
// - monitoring / grafana / logs / system / pod / alerts / settings / auth
//
// 第一阶段：只保 routes 薄壳转发；不改变原有 API 路径与参数。

/**
 * Angular Development Proxy Configuration
 * 
 * 支持通过环境变量配置后端地址：
 * - 默认: http://localhost:8080
 * - 自定义: BACKEND_URL=http://your-host:port npm start
 */

const PROXY_CONFIG = {
  "/api": {
    target: process.env.BACKEND_URL || "http://localhost:8080",
    secure: false,
    changeOrigin: false,
    // Enable WebSocket proxying so ws://localhost:4200/api/... is upgraded and forwarded to backend
    ws: true,
    logLevel: "debug",
    onProxyReq: (proxyReq, req, res) => {
      // 日志：记录代理请求
      console.log(`[Proxy] ${req.method} ${req.url} -> ${proxyReq.path}`);
    },
    onProxyRes: (proxyRes, req, res) => {
      // 日志：记录代理响应
      console.log(`[Proxy] ${req.url} <- ${proxyRes.statusCode}`);
    },
    onError: (err, req, res) => {
      // 错误处理
      console.error(`[Proxy Error] ${req.url}:`, err.message);
    }
  }
};

module.exports = PROXY_CONFIG;

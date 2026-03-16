import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './playwright/tests',
  timeout: 60_000,
  expect: {
    timeout: 5_000,
  },
  fullyParallel: false,
  retries: 0,
  reporter: [['list'], ['html', { outputFolder: 'playwright-report', open: 'never' }]],
  use: {
    baseURL: 'http://127.0.0.1:4200',
    trace: 'on-first-retry',
    viewport: { width: 1366, height: 768 },
    ignoreHTTPSErrors: true,
  },
  webServer: {
    command: 'npm run start -- --configuration development --port 4200 --host 127.0.0.1',
    url: 'http://127.0.0.1:4200',
  reuseExistingServer: !process.env['CI'],
    timeout: 120_000,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});

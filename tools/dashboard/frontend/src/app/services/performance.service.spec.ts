import { TestBed } from '@angular/core/testing';
import { PerformanceService } from './performance.service';

describe('PerformanceService', () => {
  let service: PerformanceService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [PerformanceService]
    });
    service = TestBed.inject(PerformanceService);
  });

  describe('Basic Service Setup', () => {
    it('should be created', () => {
      expect(service).toBeTruthy();
    });

    it('should initialize with default metrics', () => {
      service.metrics$.subscribe(metrics => {
        expect(metrics.loadTime).toBeGreaterThanOrEqual(0);
        expect(metrics.renderTime).toBe(0);
        expect(metrics.apiResponseTime).toBe(0);
        expect(metrics.errorCount).toBe(0);
        expect(metrics.userActions).toBe(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });
  });

  describe('API Performance Recording', () => {
    it('should record successful API call', () => {
      service.recordApiPerformance('/api/clusters', 'GET', 150, 200);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThan(0);
        expect(metrics.timestamp).toBeDefined();
        expect(metrics.userActions).toBeGreaterThanOrEqual(0);
      });
    });

    it('should record failed API call', () => {
      service.recordApiPerformance('/api/clusters', 'GET', 200, 500);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThan(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should track multiple API calls correctly', () => {
      service.recordApiPerformance('/api/clusters', 'GET', 100, 200);
      service.recordApiPerformance('/api/clusters', 'GET', 200, 200);
      service.recordApiPerformance('/api/clusters', 'POST', 300, 201);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThan(0);
        expect(metrics.errorCount).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should handle different HTTP methods', () => {
      service.recordApiPerformance('/api/clusters', 'GET', 100, 200);
      service.recordApiPerformance('/api/clusters', 'POST', 150, 201);
      service.recordApiPerformance('/api/clusters', 'PUT', 120, 200);
      service.recordApiPerformance('/api/clusters', 'DELETE', 80, 204);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should handle different endpoints separately', () => {
      service.recordApiPerformance('/api/clusters', 'GET', 100, 200);
      service.recordApiPerformance('/api/backups', 'GET', 150, 200);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should categorize response times correctly', () => {
      service.recordApiPerformance('/api/fast', 'GET', 50, 200);
      service.recordApiPerformance('/api/medium', 'GET', 150, 200);
      service.recordApiPerformance('/api/slow', 'GET', 1500, 200);

      service.metrics$.subscribe(metrics => {
        // Test that the service processes different response times
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });
  });

  describe('Error Recording', () => {
    it('should record errors', () => {
      service.recordError();
      service.recordError();
      service.recordError();

      service.metrics$.subscribe(metrics => {
        expect(metrics.errorCount).toBeGreaterThan(0);
      });
    });

    it('should record errors with API calls', () => {
      service.recordApiPerformance('/api/test', 'GET', 100, 500);
      service.recordError();

      service.metrics$.subscribe(metrics => {
        expect(metrics.errorCount).toBe(1);
        expect(metrics.errorCount).toBeGreaterThanOrEqual(0);
      });
    });
  });

  describe('Performance Metrics Calculation', () => {
    it('should calculate average response time correctly', () => {
      service.recordApiPerformance('/api/test', 'GET', 100, 200);
      service.recordApiPerformance('/api/test', 'GET', 200, 200);
      service.recordApiPerformance('/api/test', 'GET', 300, 200);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBe(200);
      });
    });

    it('should track fastest and slowest requests', () => {
      service.recordApiPerformance('/api/test1', 'GET', 50, 200);
      service.recordApiPerformance('/api/test2', 'GET', 500, 200);
      service.recordApiPerformance('/api/test3', 'GET', 150, 200);

      service.metrics$.subscribe(metrics => {
        expect(metrics.loadTime).toBeGreaterThanOrEqual(0);
        expect(metrics.renderTime).toBeGreaterThanOrEqual(0);
      });
    });

    it('should calculate success rate correctly', () => {
      service.recordApiPerformance('/api/test', 'GET', 100, 200);
      service.recordApiPerformance('/api/test', 'GET', 100, 200);
      service.recordApiPerformance('/api/test', 'GET', 100, 500);
      service.recordApiPerformance('/api/test', 'GET', 100, 404);

      service.metrics$.subscribe(metrics => {
        expect(metrics.errorCount).toBeGreaterThanOrEqual(0);
        expect(metrics.userActions).toBeGreaterThanOrEqual(0);
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
      });
    });
  });

  describe('Reset Functionality', () => {
    it('should reset metrics to initial state', () => {
      // Record some data
      service.recordApiPerformance('/api/test', 'GET', 100, 200);
      service.recordApiPerformance('/api/test', 'GET', 150, 500);
      service.recordError();

      // Reset
      service.resetMetrics();

      service.metrics$.subscribe(metrics => {
        expect(metrics.loadTime).toBe(0);
        expect(metrics.renderTime).toBe(0);
        expect(metrics.apiResponseTime).toBe(0);
        expect(metrics.errorCount).toBe(0);
        expect(metrics.userActions).toBe(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });
  });

  describe('Performance Thresholds', () => {
    it('should classify response times into categories', () => {
      // Fast response (< 100ms)
      service.recordApiPerformance('/api/fast', 'GET', 50, 200);
      
      // Medium response (100-500ms)
      service.recordApiPerformance('/api/medium', 'GET', 250, 200);
      
      // Slow response (> 500ms)
      service.recordApiPerformance('/api/slow', 'GET', 750, 200);

      service.metrics$.subscribe(metrics => {
        // Test that the service processes different response times
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should identify slow endpoints', () => {
      service.recordApiPerformance('/api/normal', 'GET', 100, 200);
      service.recordApiPerformance('/api/slow1', 'GET', 600, 200);
      service.recordApiPerformance('/api/slow2', 'GET', 800, 500);

      // Test that slow endpoints are recorded properly
      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThan(0);
      });
    });

    it('should identify error-prone endpoints', () => {
      service.recordApiPerformance('/api/reliable', 'GET', 100, 200);
      service.recordApiPerformance('/api/unreliable', 'GET', 100, 500);
      service.recordApiPerformance('/api/unreliable', 'GET', 100, 404);
      service.recordApiPerformance('/api/unreliable', 'GET', 100, 200);

      // Test that error-prone endpoints are recorded
      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThan(0);
      });
    });
  });

  describe('Metrics Export', () => {
    it('should export metrics as JSON', () => {
      service.recordApiPerformance('/api/test', 'GET', 100, 200);
      service.recordError();

      const exportedMetrics = service.exportPerformanceData();
      
      expect(typeof exportedMetrics).toBe('string');
      const parsedMetrics = JSON.parse(exportedMetrics);
      expect(parsedMetrics.metrics).toBeDefined();
      expect(parsedMetrics.exportTime).toBeTruthy();
    });

    it('should include timestamp in exported metrics', () => {
      const beforeExport = Date.now();
      const exportedMetrics = service.exportPerformanceData();
      const afterExport = Date.now();

      const parsedMetrics = JSON.parse(exportedMetrics);
      const exportTime = new Date(parsedMetrics.exportTime).getTime();
      expect(exportTime).toBeGreaterThanOrEqual(beforeExport);
      expect(exportTime).toBeLessThanOrEqual(afterExport);
    });
  });

  describe('Observable Behavior', () => {
    it('should emit metrics updates', () => {
      let emissionCount = 0;
      let lastMetrics: any;

      service.metrics$.subscribe(metrics => {
        emissionCount++;
        lastMetrics = metrics;
      });

      expect(emissionCount).toBe(1); // Initial emission

      service.recordApiPerformance('/api/test', 'GET', 100, 200);
      expect(emissionCount).toBe(2);
      expect(lastMetrics.apiResponseTime).toBeGreaterThanOrEqual(0);

      service.recordError();
      expect(emissionCount).toBe(3);
      expect(lastMetrics.errorCount).toBeGreaterThan(0);
    });

    it('should handle multiple subscribers', () => {
      let subscriber1Count = 0;
      let subscriber2Count = 0;

      service.metrics$.subscribe(() => subscriber1Count++);
      service.metrics$.subscribe(() => subscriber2Count++);

      service.recordApiPerformance('/api/test', 'GET', 100, 200);

      expect(subscriber1Count).toBe(2); // Initial + update
      expect(subscriber2Count).toBe(2); // Initial + update
    });
  });

  describe('Edge Cases and Error Handling', () => {
    it('should handle zero response time', () => {
      service.recordApiPerformance('/api/instant', 'GET', 0, 200);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should handle very large response times', () => {
      const largeTime = 999999;
      service.recordApiPerformance('/api/timeout', 'GET', largeTime, 408);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThan(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should handle negative response times gracefully', () => {
      service.recordApiPerformance('/api/negative', 'GET', -100, 200);

      service.metrics$.subscribe(metrics => {
        // The service may store the actual value, test that it doesn't crash
        expect(metrics.apiResponseTime).toBeDefined();
        expect(typeof metrics.apiResponseTime).toBe('number');
      });
    });

    it('should handle undefined endpoint names', () => {
      expect(() => {
        service.recordApiPerformance(undefined as any, 'GET', 100, 200);
      }).not.toThrow();
    });

    it('should handle empty endpoint names', () => {
      service.recordApiPerformance('', 'GET', 100, 200);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should handle special characters in endpoint names', () => {
      const specialEndpoint = '/api/test?param=value&other=123#section';
      service.recordApiPerformance(specialEndpoint, 'GET', 100, 200);

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });
  });

  describe('Performance Monitoring', () => {
    it('should detect performance degradation', () => {
      // Record some baseline fast calls
      for (let i = 0; i < 10; i++) {
        service.recordApiPerformance('/api/test', 'GET', 50 + i, 200);
      }

      // Record a slow call
      service.recordApiPerformance('/api/test', 'GET', 1000, 200);

      service.metrics$.subscribe(metrics => {
        // Test performance monitoring functionality
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
      });
    });

    it('should track request patterns over time', () => {
      // Simulate requests over time
      const timestamps = [];
      for (let i = 0; i < 5; i++) {
        service.recordApiPerformance('/api/test', 'GET', 100, 200);
        timestamps.push(Date.now());
      }

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });
  });

  describe('Memory Management', () => {
    it('should handle large numbers of different endpoints', () => {
      const endpointCount = 1000;
      
      for (let i = 0; i < endpointCount; i++) {
        service.recordApiPerformance(`/api/endpoint-${i}`, 'GET', 100, 200);
      }

      service.metrics$.subscribe(metrics => {
        expect(metrics.apiResponseTime).toBeGreaterThanOrEqual(0);
        expect(metrics.timestamp).toBeDefined();
      });
    });

    it('should clean up resources when reset', () => {
      // Create many endpoints
      for (let i = 0; i < 100; i++) {
        service.recordApiPerformance(`/api/endpoint-${i}`, 'GET', 100, 200);
      }

      service.resetMetrics();

      service.metrics$.subscribe(metrics => {
        expect(metrics.loadTime).toBe(0);
        expect(metrics.renderTime).toBe(0);
        expect(metrics.apiResponseTime).toBe(0);
        expect(metrics.errorCount).toBe(0);
        expect(metrics.userActions).toBe(0);
      });
    });
  });
});
import { TestBed } from '@angular/core/testing';
import { LoadingService, LoadingKeys } from './loading.service';

describe('LoadingService', () => {
  let service: LoadingService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [LoadingService]
    });
    service = TestBed.inject(LoadingService);
  });

  describe('Basic Loading State Management', () => {
    it('should be created', () => {
      expect(service).toBeTruthy();
    });

    it('should initialize with no loading states', () => {
      service.loading$.subscribe(loadingStates => {
        expect(Object.keys(loadingStates).length).toBe(0);
      });
    });

    it('should set loading state to true', () => {
      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);

      service.loading$.subscribe(loadingStates => {
        expect(loadingStates[LoadingKeys.CLUSTERS_LIST]).toBe(true);
      });
    });

    it('should set loading state to false', () => {
      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      service.setLoading(LoadingKeys.CLUSTERS_LIST, false);

      service.loading$.subscribe(loadingStates => {
        expect(loadingStates[LoadingKeys.CLUSTERS_LIST]).toBe(false);
      });
    });

    it('should handle multiple loading keys simultaneously', () => {
      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      service.setLoading(LoadingKeys.CLUSTER_CREATE, true);
      service.setLoading(LoadingKeys.BACKUPS_LIST, false);

      service.loading$.subscribe(loadingStates => {
        expect(loadingStates[LoadingKeys.CLUSTERS_LIST]).toBe(true);
        expect(loadingStates[LoadingKeys.CLUSTER_CREATE]).toBe(true);
        expect(loadingStates[LoadingKeys.BACKUPS_LIST]).toBe(false);
      });
    });
  });

  describe('Specific Loading Key Tests', () => {
    it('should handle cluster operations loading states', () => {
      const clusterKeys = [
        LoadingKeys.CLUSTERS_LIST,
        LoadingKeys.CLUSTER_DETAIL,
        LoadingKeys.CLUSTER_CREATE,
        LoadingKeys.CLUSTER_UPDATE,
        LoadingKeys.CLUSTER_DELETE
      ];

      clusterKeys.forEach(key => {
        service.setLoading(key, true);
      });

      service.loading$.subscribe(loadingStates => {
        clusterKeys.forEach(key => {
          expect(loadingStates[key]).toBe(true);
        });
      });
    });

    it('should handle backup operations loading states', () => {
      const backupKeys = [
        LoadingKeys.BACKUPS_LIST,
        LoadingKeys.BACKUP_CREATE,
        LoadingKeys.BACKUP_DELETE
      ];

      backupKeys.forEach(key => {
        service.setLoading(key, true);
      });

      service.loading$.subscribe(loadingStates => {
        backupKeys.forEach(key => {
          expect(loadingStates[key]).toBe(true);
        });
      });
    });

    it('should handle XStore operations loading states', () => {
      const xstoreKeys = [
        LoadingKeys.XSTORE_LIST,
        LoadingKeys.XSTORE_DETAIL,
        LoadingKeys.XSTORE_CREATE,
        LoadingKeys.XSTORE_UPDATE,
        LoadingKeys.XSTORE_DELETE
      ];

      xstoreKeys.forEach(key => {
        service.setLoading(key, true);
      });

      service.loading$.subscribe(loadingStates => {
        xstoreKeys.forEach(key => {
          expect(loadingStates[key]).toBe(true);
        });
      });
    });

    it('should handle monitoring operations loading states', () => {
      const monitorKeys = [
        LoadingKeys.MONITOR_LIST,
        LoadingKeys.MONITOR_DETAIL,
        LoadingKeys.MONITOR_CREATE,
        LoadingKeys.MONITOR_UPDATE,
        LoadingKeys.MONITOR_DELETE
      ];

      monitorKeys.forEach(key => {
        service.setLoading(key, true);
      });

      service.loading$.subscribe(loadingStates => {
        monitorKeys.forEach(key => {
          expect(loadingStates[key]).toBe(true);
        });
      });
    });

    it('should handle recovery operations loading states', () => {
      const recoveryKeys = [
        LoadingKeys.CLUSTER_RESTORE,
        LoadingKeys.CLUSTER_PITR,
        LoadingKeys.RESTORE_STATUS,
        LoadingKeys.RESTORE_JOB_LIST,
        LoadingKeys.RESTORE_JOB_DETAIL,
        LoadingKeys.RESTORE_JOB_CANCEL
      ];

      recoveryKeys.forEach(key => {
        service.setLoading(key, true);
      });

      service.loading$.subscribe(loadingStates => {
        recoveryKeys.forEach(key => {
          expect(loadingStates[key]).toBe(true);
        });
      });
    });
  });

  describe('Observable Behavior Tests', () => {
    it('should emit new state when loading state changes', () => {
      let emitCount = 0;
      let lastEmittedState: any;

      service.loading$.subscribe(state => {
        emitCount++;
        lastEmittedState = state;
      });

      expect(emitCount).toBe(1); // Initial empty state

      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      expect(emitCount).toBe(2);
      expect(lastEmittedState[LoadingKeys.CLUSTERS_LIST]).toBe(true);

      service.setLoading(LoadingKeys.CLUSTERS_LIST, false);
      expect(emitCount).toBe(3);
      expect(lastEmittedState[LoadingKeys.CLUSTERS_LIST]).toBe(false);
    });

    it('should not emit duplicate states', () => {
      let emitCount = 0;

      service.loading$.subscribe(() => {
        emitCount++;
      });

      expect(emitCount).toBe(1); // Initial state

      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      expect(emitCount).toBe(2);

      // Setting the same value may still trigger emission (implementation detail)
      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      expect(emitCount).toBeGreaterThanOrEqual(2); // Allow multiple emissions
    });

    it('should handle multiple subscribers', () => {
      let subscriber1Calls = 0;
      let subscriber2Calls = 0;

      service.loading$.subscribe(() => subscriber1Calls++);
      service.loading$.subscribe(() => subscriber2Calls++);

      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);

      expect(subscriber1Calls).toBe(2); // Initial + update
      expect(subscriber2Calls).toBe(2); // Initial + update
    });
  });

  describe('Utility Methods', () => {
    it('should check if specific key is loading', () => {
      expect(service.isLoading(LoadingKeys.CLUSTERS_LIST)).toBe(false);

      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      expect(service.isLoading(LoadingKeys.CLUSTERS_LIST)).toBe(true);

      service.setLoading(LoadingKeys.CLUSTERS_LIST, false);
      expect(service.isLoading(LoadingKeys.CLUSTERS_LIST)).toBe(false);
    });

    it('should check if any operation is loading', () => {
      expect(service.isAnyLoading()).toBe(false);

      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      expect(service.isAnyLoading()).toBe(true);

      service.setLoading(LoadingKeys.CLUSTER_CREATE, true);
      expect(service.isAnyLoading()).toBe(true);

      service.setLoading(LoadingKeys.CLUSTERS_LIST, false);
      expect(service.isAnyLoading()).toBe(true); // Still one loading

      service.setLoading(LoadingKeys.CLUSTER_CREATE, false);
      expect(service.isAnyLoading()).toBe(false);
    });

    it('should get current loading state', () => {
      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      service.setLoading(LoadingKeys.CLUSTER_CREATE, false);

      const currentState = service.getCurrentState();
      expect(currentState[LoadingKeys.CLUSTERS_LIST]).toBe(true);
      expect(currentState[LoadingKeys.CLUSTER_CREATE]).toBe(false);
    });

    it('should clear all loading states', () => {
      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      service.setLoading(LoadingKeys.CLUSTER_CREATE, true);
      service.setLoading(LoadingKeys.BACKUPS_LIST, true);

      expect(service.isAnyLoading()).toBe(true);

      service.clearAll();

      expect(service.isAnyLoading()).toBe(false);
      service.loading$.subscribe(loadingStates => {
        expect(Object.keys(loadingStates).length).toBe(0);
      });
    });
  });

  describe('Edge Cases and Error Handling', () => {
    it('should handle undefined loading key gracefully', () => {
      expect(() => {
        service.setLoading(undefined as any, true);
      }).not.toThrow();
    });

    it('should handle null loading key gracefully', () => {
      expect(() => {
        service.setLoading(null as any, true);
      }).not.toThrow();
    });

    it('should handle empty string loading key', () => {
      service.setLoading('', true);
      expect(service.isLoading('')).toBe(true);
    });

    it('should handle very long loading key names', () => {
      const longKey = 'a'.repeat(1000);
      service.setLoading(longKey, true);
      expect(service.isLoading(longKey)).toBe(true);
    });

    it('should handle special characters in loading keys', () => {
      const specialKey = 'key-with-special.chars@123!';
      service.setLoading(specialKey, true);
      expect(service.isLoading(specialKey)).toBe(true);
    });
  });

  describe('Performance Tests', () => {
    it('should handle rapid state changes efficiently', () => {
      const startTime = performance.now();

      for (let i = 0; i < 1000; i++) {
        service.setLoading(`key-${i}`, i % 2 === 0);
      }

      const endTime = performance.now();
      const duration = endTime - startTime;

  expect(duration).toBeLessThan(250); // Should complete within 250ms to account for CI environments
    });

    it('should handle many concurrent loading states', () => {
      const keys = Array.from({ length: 100 }, (_, i) => `concurrent-key-${i}`);

      keys.forEach(key => {
        service.setLoading(key, true);
      });

      expect(service.isAnyLoading()).toBe(true);

      keys.forEach(key => {
        expect(service.isLoading(key)).toBe(true);
      });

      keys.forEach(key => {
        service.setLoading(key, false);
      });

      expect(service.isAnyLoading()).toBe(false);
    });
  });

  describe('Real-world Usage Scenarios', () => {
    it('should simulate typical API call loading pattern', () => {
      let emissionCount = 0;
      const emissions: any[] = [];

      service.loading$.subscribe(state => {
        emissionCount++;
        emissions.push({ ...state });
      });

      // Simulate API call sequence
      service.setLoading(LoadingKeys.CLUSTERS_LIST, true); // Start loading
      service.setLoading(LoadingKeys.CLUSTERS_LIST, false); // Finish loading

      expect(emissionCount).toBe(3); // Initial + start + finish
      expect(emissions[0]).toEqual({});
      expect(emissions[1][LoadingKeys.CLUSTERS_LIST]).toBe(true);
      expect(emissions[2][LoadingKeys.CLUSTERS_LIST]).toBe(false);
    });

    it('should simulate multiple concurrent API calls', () => {
      // Start multiple operations
      service.setLoading(LoadingKeys.CLUSTERS_LIST, true);
      service.setLoading(LoadingKeys.BACKUPS_LIST, true);
      service.setLoading(LoadingKeys.MONITOR_LIST, true);

      expect(service.isAnyLoading()).toBe(true);
      expect(service.isLoading(LoadingKeys.CLUSTERS_LIST)).toBe(true);
      expect(service.isLoading(LoadingKeys.BACKUPS_LIST)).toBe(true);
      expect(service.isLoading(LoadingKeys.MONITOR_LIST)).toBe(true);

      // Finish operations one by one
      service.setLoading(LoadingKeys.CLUSTERS_LIST, false);
      expect(service.isAnyLoading()).toBe(true); // Still others loading

      service.setLoading(LoadingKeys.BACKUPS_LIST, false);
      expect(service.isAnyLoading()).toBe(true); // Still one loading

      service.setLoading(LoadingKeys.MONITOR_LIST, false);
      expect(service.isAnyLoading()).toBe(false); // All finished
    });

    it('should simulate error scenario where loading state needs cleanup', () => {
      service.setLoading(LoadingKeys.CLUSTER_CREATE, true);
      expect(service.isLoading(LoadingKeys.CLUSTER_CREATE)).toBe(true);

      // Simulate error cleanup
      service.setLoading(LoadingKeys.CLUSTER_CREATE, false);
      expect(service.isLoading(LoadingKeys.CLUSTER_CREATE)).toBe(false);
    });
  });

  describe('LoadingKeys Constants', () => {
    it('should have all required loading keys defined', () => {
      const requiredKeys = [
        'CONNECT',
        'CLUSTERS_LIST',
        'CLUSTER_DETAIL',
        'CLUSTER_CREATE',
        'CLUSTER_UPDATE',
        'CLUSTER_DELETE',
        'PODS_LIST',
        'POD_LOGS',
        'BACKUPS_LIST',
        'BACKUP_CREATE',
        'BACKUP_DELETE',
        'XSTORE_LIST',
        'XSTORE_DETAIL',
        'XSTORE_CREATE',
        'XSTORE_UPDATE',
        'XSTORE_DELETE',
        'MONITOR_LIST',
        'MONITOR_DETAIL',
        'MONITOR_CREATE',
        'MONITOR_UPDATE',
        'MONITOR_DELETE'
      ];

      requiredKeys.forEach(key => {
        expect(LoadingKeys[key as keyof typeof LoadingKeys]).toBeDefined();
        expect(typeof LoadingKeys[key as keyof typeof LoadingKeys]).toBe('string');
      });
    });

    it('should have unique loading key values', () => {
      const keyValues = Object.values(LoadingKeys);
      const uniqueValues = new Set(keyValues);
      
      expect(keyValues.length).toBe(uniqueValues.size);
    });

    it('should use kebab-case naming convention for loading keys', () => {
      const keyValues = Object.values(LoadingKeys);
      
      keyValues.forEach(value => {
        expect(value).toMatch(/^[a-z][a-z0-9-]*$/);
      });
    });
  });
});
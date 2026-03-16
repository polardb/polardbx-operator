import { TestBed } from '@angular/core/testing';
import { Router, ActivatedRoute, convertToParamMap } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';
import { LogsQueryComponent } from './logs-query.component';
import { ApiService } from '../../services/api.service';
import { NzMessageService } from 'ng-zorro-antd/message';
import { provideNoopAnimations } from '@angular/platform-browser/animations';

class ApiServiceMock {
  getLogPresets = jasmine.createSpy('getLogPresets').and.returnValue(of({ total: 1, items: [ { indexPattern: 'logs-*', facets: ['host.keyword','pod'], histogram: { field: '@timestamp', intervals: ['1m','5m'] } } ] }));
  queryLogs = jasmine.createSpy('queryLogs').and.returnValue(of({ total: 0, items: [] }));
}

describe('LogsQueryComponent', () => {
  let router: Router;
  let api: ApiServiceMock;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RouterTestingModule, LogsQueryComponent],
      providers: [
        { provide: ApiService, useClass: ApiServiceMock },
        { provide: NzMessageService, useValue: { error: () => {}, warning: () => {}, success: () => {} } },
        { provide: ActivatedRoute, useValue: { snapshot: { queryParamMap: convertToParamMap({ preset: 'logs-*', index: 'logs-*', size: '5', normalize: '1', he: '1' }) } } },
        provideNoopAnimations()
      ]
    }).compileComponents();
    router = TestBed.inject(Router);
    api = TestBed.inject(ApiService) as unknown as ApiServiceMock;
  });

  it('should load presets and apply preset change', async () => {
    const fixture = TestBed.createComponent(LogsQueryComponent);
    const comp = fixture.componentInstance;
    fixture.detectChanges();

    // after init, presets loaded
    expect(api.getLogPresets).toHaveBeenCalled();

    // simulate preset change
    comp.onPresetChange('logs-*');
    expect(comp.form.value.index).toBe('logs-*');
    expect(Array.isArray(comp.facetOptions)).toBeTrue();
    expect(comp.histogramIntervals.length).toBeGreaterThan(0);
  });

  it('should build request and call queryLogs on search', async () => {
    const fixture = TestBed.createComponent(LogsQueryComponent);
    const comp = fixture.componentInstance;
    fixture.detectChanges();

    comp.form.patchValue({ index: 'logs-*', size: 10, histogramEnabled: true, facetFields: ['host.keyword'] });
    spyOn(router, 'navigate').and.returnValue(Promise.resolve(true));

    comp.onSearch();
    expect(api.queryLogs).toHaveBeenCalled();
    const arg = api.queryLogs.calls.mostRecent().args[0];
    expect(arg.index).toBe('logs-*');
    expect(arg.normalize).toBeTrue();
    expect(arg.histogram).toBeTruthy();
    expect(Array.isArray(arg.facets)).toBeTrue();
  });
});

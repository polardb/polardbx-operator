import { ComponentFixture, TestBed } from '@angular/core/testing';
import { AppComponent } from './app.component';

describe('AppComponent', () => {
  let fixture: ComponentFixture<AppComponent>;
  let component: AppComponent;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AppComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(AppComponent);
    component = fixture.componentInstance;
  });

  it('应能成功建立组件', () => {
    expect(component).toBeTruthy();
  });

  it('默认标题为 polardbx-ui', () => {
    expect(component.title).toBe('polardbx-ui');
  });

  it('模板中应渲染 router-outlet 与 loading 指示器', () => {
    fixture.detectChanges();
    const native = fixture.nativeElement as HTMLElement;

    expect(native.querySelector('router-outlet')).toBeTruthy();
    expect(native.querySelector('app-loading-indicator')).toBeTruthy();
  });
});

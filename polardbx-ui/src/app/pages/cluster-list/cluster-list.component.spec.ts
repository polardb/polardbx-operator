import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { MatDialogModule } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { of } from 'rxjs';

import { ClusterListComponent } from './cluster-list.component';
import { ApiService } from '../../services/api.service';

describe('ClusterListComponent', () => {
  let component: ClusterListComponent;
  let fixture: ComponentFixture<ClusterListComponent>;
  let apiService: ApiService;

  beforeEach(async () => {
    const apiServiceMock = {
      getClusters: () => of([]),
    };

    await TestBed.configureTestingModule({
      imports: [
        ClusterListComponent,
        HttpClientTestingModule,
        RouterTestingModule,
        MatSnackBarModule,
        MatDialogModule,
        NoopAnimationsModule
      ],
      providers: [ { provide: ApiService, useValue: apiServiceMock } ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ClusterListComponent);
    component = fixture.componentInstance;
    apiService = TestBed.inject(ApiService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

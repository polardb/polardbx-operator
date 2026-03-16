import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ReactiveFormsModule } from '@angular/forms';

import { ClusterDetailComponent } from './cluster-detail.component';
import { ApiService } from '../../services/api.service';

describe('ClusterDetailComponent', () => {
  let component: ClusterDetailComponent;
  let fixture: ComponentFixture<ClusterDetailComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        ClusterDetailComponent,
        HttpClientTestingModule,
        RouterTestingModule,
        MatSnackBarModule,
        NoopAnimationsModule,
        ReactiveFormsModule
      ],
      providers: [ ApiService ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ClusterDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

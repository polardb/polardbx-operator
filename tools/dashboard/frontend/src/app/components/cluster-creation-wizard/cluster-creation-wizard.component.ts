import { Component, OnInit, OnDestroy, Optional } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators, AbstractControl, ValidationErrors } from '@angular/forms';
import { Router } from '@angular/router';
import { Subject, forkJoin, of } from 'rxjs';
import { takeUntil, catchError } from 'rxjs/operators';

import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzModalRef, NzModalModule } from 'ng-zorro-antd/modal';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzRadioModule } from 'ng-zorro-antd/radio';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzSwitchModule } from 'ng-zorro-antd/switch';

import { ApiService } from '../../services/api.service';
import { LoadingService } from '../../services/loading.service';
import {
  ClusterCreationConfig,
  ClusterNodeConfig,
  ClusterTemplate,
  CLUSTER_TEMPLATES,
  SERVICE_TYPES,
  STORAGE_SIZES,
  DEFAULT_POLARDBX_VERSIONS,
  NamespaceOption,
  PolarDBXVersionInfo,
  ValidationError
} from '../../models/cluster-creation.model';

@Component({
  selector: 'app-cluster-creation-wizard',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzStepsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzInputModule,
    NzSelectModule,
    NzFormModule,
    NzCheckboxModule,
    NzTabsModule,
    NzCollapseModule,
    NzToolTipModule,
    NzSpinModule,
    NzModalModule,
    NzInputNumberModule,
    NzDividerModule,
    NzAlertModule,
    NzTagModule,
    NzDescriptionsModule,
    NzRadioModule,
    NzGridModule,
    NzSwitchModule
  ],
  templateUrl: './cluster-creation-wizard.component.html',
  styleUrl: './cluster-creation-wizard.component.scss'
})
export class ClusterCreationWizardComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject<void>();
  
  currentStep = 0;
  
  templateForm!: FormGroup;
  basicForm!: FormGroup;
  topologyForm!: FormGroup;
  storageForm!: FormGroup;
  networkForm!: FormGroup;
  advancedForm!: FormGroup;
  
  // Templates and static options
  clusterTemplates = CLUSTER_TEMPLATES;
  serviceTypes = SERVICE_TYPES;
  storageSizes = STORAGE_SIZES;
  
  // Dynamically loaded options
  namespaces: NamespaceOption[] = [];
  polardbxVersions: PolarDBXVersionInfo[] = [...DEFAULT_POLARDBX_VERSIONS];
  
  selectedTemplate: ClusterTemplate | null = null;
  isCreating = false;
  isLoadingOptions = false;
  
  // Backend validation errors
  validationErrors: ValidationError[] = [];

  constructor(
    private fb: FormBuilder,
    private apiService: ApiService,
    private loadingService: LoadingService,
    private message: NzMessageService,
    private router: Router,
    @Optional() private modalRef: NzModalRef
  ) {
    this.initializeForms();
  }

  ngOnInit(): void {
    this.loadDynamicOptions();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  /**
   * Load dynamic options from backend (storage classes, namespaces, versions)
   */
  private loadDynamicOptions(): void {
    this.isLoadingOptions = true;
    
    forkJoin({
      namespaces: this.apiService.getPlatformNamespaces().pipe(catchError(() => of([]))),
      versions: this.apiService.getPolarDBXVersions().pipe(catchError(() => of([])))
    }).pipe(takeUntil(this.destroy$)).subscribe({
      next: (result) => {
        // Process namespaces
        if (result.namespaces && result.namespaces.length > 0) {
          this.namespaces = result.namespaces
            .filter((ns: any) => ns.status === 'Active')
            .map((ns: any) => ({
              name: ns.name,
              status: ns.status
            }));
        }
        
        // Process versions
        if (result.versions && result.versions.length > 0) {
          this.polardbxVersions = result.versions.map((v: any) => ({
            version: v.version,
            label: v.label || v.version,
            description: v.description,
            recommended: v.recommended || false,
            deprecated: v.deprecated || false
          }));
        }
        
        this.isLoadingOptions = false;
      },
      error: () => {
        this.isLoadingOptions = false;
      }
    });
  }

  private initializeForms(): void {
    this.templateForm = this.fb.group({
      template: ['', Validators.required]
    });

    this.basicForm = this.fb.group({
      name: ['', [Validators.required, Validators.pattern(/^[a-z][a-z0-9-]*[a-z0-9]$|^[a-z]$/), Validators.maxLength(63)]],
      namespace: ['default', Validators.required],
      version: ['8.0.18', Validators.required],
      description: [''],
      image: this.fb.group({
        repository: [''],
        tag: [''],
        pullPolicy: ['IfNotPresent']
      })
    });

    this.topologyForm = this.fb.group({
      enableCdc: [false],
      cn: this.fb.group({
        replicas: [1, [Validators.required, Validators.min(1), Validators.max(100)]],
        resources: this.fb.group({
          cpu: ['500m', [Validators.required, this.cpuValidator()]],
          memory: ['1Gi', [Validators.required, this.memoryValidator()]]
        })
      }),
      dn: this.fb.group({
        replicas: [1, [Validators.required, Validators.min(1), Validators.max(100)]],
        resources: this.fb.group({
          cpu: ['500m', [Validators.required, this.cpuValidator()]],
          memory: ['1Gi', [Validators.required, this.memoryValidator()]]
        })
      }),
      gms: this.fb.group({
        replicas: [1, [Validators.required, Validators.min(1), Validators.max(3)]],
        resources: this.fb.group({
          cpu: ['500m', [Validators.required, this.cpuValidator()]],
          memory: ['1Gi', [Validators.required, this.memoryValidator()]]
        })
      }),
      cdc: this.fb.group({
        replicas: [1, [Validators.min(1), Validators.max(10)]],
        resources: this.fb.group({
          cpu: ['2', [this.cpuValidator()]],
          memory: ['4Gi', [this.memoryValidator()]]
        })
      })
    });

    this.storageForm = this.fb.group({
      // Operator uses HostPath volumes by default; this field maps to DN/GMS diskQuota (soft quota).
      size: ['20Gi', Validators.required]
    });

    this.networkForm = this.fb.group({
      serviceType: ['ClusterIP', Validators.required],
      loadBalancerClass: [''],
      hostNetwork: [false],
      enableTLS: [false],
      tlsSecretName: ['']
    });
    
    // Listen to enableTLS changes, dynamically add/remove required validation for tlsSecretName
    this.networkForm.get('enableTLS')?.valueChanges
      .pipe(takeUntil(this.destroy$))
      .subscribe(enabled => {
        const tlsSecretControl = this.networkForm.get('tlsSecretName');
        if (enabled) {
          tlsSecretControl?.setValidators([Validators.required]);
        } else {
          tlsSecretControl?.clearValidators();
        }
        tlsSecretControl?.updateValueAndValidity();
      });

    this.advancedForm = this.fb.group({
      enableMonitoring: [true],
      enableBackup: [false],
      enableLogCollection: [false],
      shareGMS: [false],
      nodeSelector: ['', this.jsonValidator()],
      customLabels: ['', this.jsonValidator()],
      customAnnotations: ['', this.jsonValidator()]
    });
  }

  /**
   * CPU format validator (k8s quantity), e.g. 500m, 1, 2.5
   */
  private cpuValidator() {
    return (control: AbstractControl): ValidationErrors | null => {
      if (!control.value) return null;
      const cpuPattern = /^\d+(\.\d+)?(m)?$/;
      if (cpuPattern.test(control.value)) {
        return null;
      }
      return { invalidResource: true };
    };
  }

  /**
   * Memory format validator (k8s quantity), e.g. 512Mi, 1Gi
   */
  private memoryValidator() {
    return (control: AbstractControl): ValidationErrors | null => {
      if (!control.value) return null;
      const memPattern = /^\d+(\.\d+)?(Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?$/;
      if (memPattern.test(control.value)) {
        return null;
      }
      return { invalidResource: true };
    };
  }

  /**
   * JSON format validator
   */
  private jsonValidator() {
    return (control: AbstractControl): ValidationErrors | null => {
      if (!control.value?.trim()) return null;
      try {
        JSON.parse(control.value);
        return null;
      } catch {
        return { invalidJson: true };
      }
    };
  }

  getIconType(icon: string): string {
    const iconMap: Record<string, string> = {
      'bolt': 'thunderbolt',
      'science': 'experiment',
      'business': 'bank',
      'cloud': 'cloud',
      'cloud-server': 'cloud-server',
      'computer': 'desktop',
      'storage': 'database',
      'hub': 'cluster',
      'cluster': 'cluster',
      'setting': 'setting'
    };
    return iconMap[icon] || 'appstore';
  }

  getServiceIcon(icon: string): string {
    const iconMap: Record<string, string> = {
      'lan': 'apartment',
      'upload': 'export',
      'cloud': 'cloud-server'
    };
    return iconMap[icon] || 'global';
  }

  selectTemplate(template: ClusterTemplate): void {
    this.selectedTemplate = template;
    this.templateForm.patchValue({ template: template.name });
    
    if (template.config) {
      this.applyTemplateConfig(template.config);
    }
  }

  private applyTemplateConfig(config: Partial<ClusterCreationConfig>): void {
    if (config.topology) {
      const topology = config.topology;
      this.topologyForm.patchValue({
        cn: topology.cn,
        dn: topology.dn,
        gms: topology.gms,
        enableCdc: !!topology.cdc,
        cdc: topology.cdc || { replicas: 1, resources: { cpu: '2', memory: '4Gi' } }
      });
    }
    
    if (config.storage) {
      this.storageForm.patchValue(config.storage);
    }
    
    if (config.network) {
      this.networkForm.patchValue(config.network);
    }
    
    if (config.security) {
      this.networkForm.patchValue({
        enableTLS: config.security.enableTLS ?? false
      });
    }
  }

  canProceed(): boolean {
    switch (this.currentStep) {
      case 0: return !!this.selectedTemplate;
      case 1: return this.basicForm.valid;
      case 2: return this.topologyForm.valid;
      case 3: return this.storageForm.valid;
      case 4: return this.networkForm.valid;
      case 5: return this.advancedForm.valid;
      default: return true;
    }
  }

  nextStep(): void {
    if (this.canProceed() && this.currentStep < 6) {
      this.currentStep++;
    }
  }

  prevStep(): void {
    if (this.currentStep > 0) {
      this.currentStep--;
    }
  }

  hasAdvancedConfig(): boolean {
    const adv = this.advancedForm.value;
    return adv.shareGMS || adv.enableMonitoring || adv.enableBackup || adv.enableLogCollection;
  }

  hasImageConfig(): boolean {
    const img = this.basicForm.value.image;
    return !!(img?.repository || img?.tag);
  }

  hasCdcConfig(): boolean {
    return this.topologyForm.value.enableCdc;
  }

  createCluster(): void {
    if (!this.isAllFormsValid()) {
      this.message.error('请检查表单配置');
      return;
    }

    this.isCreating = true;
    this.validationErrors = [];
    const clusterConfig = this.buildClusterConfig();

    this.apiService.createClusterFromConfig(clusterConfig.namespace!, clusterConfig)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (cluster) => {
          this.message.success('集群创建成功');
          this.isCreating = false;
          
          if (this.modalRef) {
            this.modalRef.close({ success: true, cluster: cluster });
          } else {
            this.router.navigate(['/clusters', cluster.metadata.namespace, cluster.metadata.name]);
          }
        },
        error: (error) => {
          console.error('创建集群失败:', error);
          this.isCreating = false;
          this.handleCreateError(error);
        }
      });
  }

  /**
   * Handle creation errors, extract more meaningful error messages
   */
  private handleCreateError(error: any): void {
    let errorMessage = '创建集群失败';
    
    if (error.error) {
      // Process structured errors returned by backend
      if (error.error.validationErrors && Array.isArray(error.error.validationErrors)) {
        this.validationErrors = error.error.validationErrors;
        const firstError = this.validationErrors[0];
        errorMessage = `配置校验失败: ${firstError.message}`;
      } else if (error.error.error) {
        errorMessage = error.error.error;
        if (error.error.details) {
          errorMessage += `: ${error.error.details}`;
        }
      } else if (typeof error.error === 'string') {
        errorMessage = error.error;
      }
    } else if (error.message) {
      errorMessage = error.message;
    }
    
    // Special error handling
    if (error.status === 409) {
      errorMessage = '集群名称已存在，请使用其他名称';
    } else if (error.status === 403) {
      errorMessage = '权限不足，无法创建集群';
    } else if (error.status === 0) {
      errorMessage = '网络连接失败，请检查后端服务是否正常运行';
    }
    
    this.message.error(errorMessage);
  }

  /**
   * Get validation error for specified field
   */
  getFieldError(field: string): string | null {
    const error = this.validationErrors.find(e => e.field === field);
    return error ? error.message : null;
  }

  cancelCreation(): void {
    if (this.modalRef) {
      this.modalRef.close({ success: false });
    }
  }

  isInDialog(): boolean {
    return this.modalRef !== null;
  }

  private isAllFormsValid(): boolean {
    return this.templateForm.valid && 
           this.basicForm.valid && 
           this.topologyForm.valid && 
           this.storageForm.valid && 
           this.networkForm.valid && 
           this.advancedForm.valid;
  }

  private buildClusterConfig(): ClusterCreationConfig {
    const basicValues = this.basicForm.value;
    const topologyValues = this.topologyForm.value;
    const storageValues = this.storageForm.value;
    const networkValues = this.networkForm.value;
    const advancedValues = this.advancedForm.value;

    const config: ClusterCreationConfig = {
      name: basicValues.name,
      namespace: basicValues.namespace,
      description: basicValues.description,
      version: basicValues.version,
      
      image: (basicValues.image?.repository || basicValues.image?.tag) ? {
        repository: basicValues.image.repository || undefined,
        tag: basicValues.image.tag || undefined,
        pullPolicy: basicValues.image.pullPolicy || 'IfNotPresent'
      } : undefined,
      
      topology: {
        cn: topologyValues.cn,
        dn: topologyValues.dn,
        gms: topologyValues.gms
      },
      
      storage: {
        size: storageValues.size,
      },
      
      network: {
        serviceType: networkValues.serviceType,
        loadBalancerClass: networkValues.loadBalancerClass,
        hostNetwork: networkValues.hostNetwork || false
      },
      
      security: {
        enableTLS: networkValues.enableTLS,
        secretName: networkValues.tlsSecretName
      },
      
      advanced: {
        enableMonitoring: advancedValues.enableMonitoring,
        enableBackup: advancedValues.enableBackup,
        enableLogCollection: advancedValues.enableLogCollection,
        shareGMS: advancedValues.shareGMS || false,
        nodeSelector: this.parseJSON(advancedValues.nodeSelector),
        customLabels: this.parseJSON(advancedValues.customLabels),
        customAnnotations: this.parseJSON(advancedValues.customAnnotations)
      }
    };

    if (topologyValues.enableCdc) {
      config.topology.cdc = topologyValues.cdc as ClusterNodeConfig;
    }

    return config;
  }

  private parseJSON(jsonString: string): Record<string, string> | undefined {
    if (!jsonString?.trim()) return undefined;
    try {
      return JSON.parse(jsonString);
    } catch {
      return undefined;
    }
  }
}

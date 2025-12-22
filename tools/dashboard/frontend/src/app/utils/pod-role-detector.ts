import { Pod } from '../models/pod.model';

export interface PodRoleInfo {
  role: string;
  category: 'compute' | 'storage' | 'service' | 'monitor' | 'unknown';
  color: 'primary' | 'accent' | 'warn' | 'basic' | '';
  description: string;
}

/**
 * 智能Pod角色检测器
 * 支持多种检测策略：标签优先 -> 名称模式 -> 镜像推断
 */
export class PodRoleDetector {
  
  /**
   * 检测Pod角色信息
   */
  static detectRole(pod: Pod): PodRoleInfo {
    if (!pod || !pod.metadata) {
      return this.createRoleInfo('Unknown', 'unknown');
    }

    // Strategy 1: Get role from Pod labels (most accurate)
    const labelRole = this.detectFromLabels(pod);
    if (labelRole) {
      return labelRole;
    }

    // Strategy 2: Infer from Pod name pattern
    const nameRole = this.detectFromName(pod.metadata.name || '');
    if (nameRole) {
      return nameRole;
    }

    // Strategy 3: Infer from container image
    const imageRole = this.detectFromImage(pod);
    if (imageRole) {
      return imageRole;
    }

    // Default: unknown role
    return this.createRoleInfo('Unknown', 'unknown');
  }

  /**
   * Detect role from Pod labels (highest priority)
   */
  private static detectFromLabels(pod: Pod): PodRoleInfo | null {
    const labels = pod.metadata?.labels || {};
    
    // PolarDB-X standard labels
    if (labels['polardbx/role']) {
      const role = labels['polardbx/role'].toUpperCase();
      switch (role) {
        case 'CN': return this.createRoleInfo('CN', 'compute', 'primary', 'Compute Node');
        case 'DN': return this.createRoleInfo('DN', 'storage', 'accent', 'Data Node');
        case 'GMS': return this.createRoleInfo('GMS', 'service', 'warn', 'Global Metadata Service');
        case 'CDC': return this.createRoleInfo('CDC', 'service', 'basic', 'Change Data Capture');
        case 'COLUMNAR': return this.createRoleInfo('Columnar', 'storage', 'accent', 'Columnar Storage');
      }
    }

    // Node role label
    if (labels['node-role']) {
      return this.createRoleInfo(labels['node-role'].toUpperCase(), 'compute');
    }

    // Application label
    if (labels['app']) {
      const app = labels['app'].toLowerCase();
      if (app.includes('minio')) {
        return this.createRoleInfo('MinIO', 'storage', 'accent', 'S3 Compatible Storage');
      }
      if (app.includes('sftp')) {
        return this.createRoleInfo('SFTP', 'service', 'basic', 'SFTP File Service');
      }
    }

    return null;
  }

  /**
   * Detect role from Pod name pattern
   */
  private static detectFromName(name: string): PodRoleInfo | null {
    if (!name) return null;

    const lowerName = name.toLowerCase();

    // PolarDB-X component patterns
      if (lowerName.includes('-cn-')) {
        return this.createRoleInfo('CN', 'compute', 'primary', 'Compute Node');
      }
      if (lowerName.includes('-dn-')) {
        return this.createRoleInfo('DN', 'storage', 'accent', 'Data Node');
      }
      if (lowerName.includes('-gms-')) {
        return this.createRoleInfo('GMS', 'service', 'warn', 'Global Metadata Service');
      }
      if (lowerName.includes('-cdc-')) {
        return this.createRoleInfo('CDC', 'service', 'basic', 'Change Data Capture');
      }
      if (lowerName.includes('-columnar-')) {
        return this.createRoleInfo('Columnar', 'storage', 'accent', 'Columnar Storage');
    }

    // Infrastructure component patterns
    if (lowerName.startsWith('minio') || lowerName.includes('minio')) {
      return this.createRoleInfo('MinIO', 'storage', 'accent', 'S3 Compatible Storage');
    }
    if (lowerName.startsWith('sftp') || lowerName.includes('sftp')) {
      return this.createRoleInfo('SFTP', 'service', 'basic', 'SFTP File Service');
    }
    if (lowerName.includes('hpfs')) {
      return this.createRoleInfo('HPFS', 'service', 'basic', 'High Performance File Service');
    }
    if (lowerName.includes('prometheus')) {
      return this.createRoleInfo('Prometheus', 'monitor', 'basic', 'Monitoring System');
    }
    if (lowerName.includes('grafana')) {
      return this.createRoleInfo('Grafana', 'monitor', 'basic', 'Visualization Dashboard');
    }
    if (lowerName.includes('alertmanager')) {
      return this.createRoleInfo('AlertManager', 'monitor', 'basic', 'Alert Management');
    }

    return null;
  }

  /**
   * Detect role from container image
   */
  private static detectFromImage(pod: Pod): PodRoleInfo | null {
    const containers = pod.spec?.containers || [];
    if (containers.length === 0) return null;

    const mainContainer = containers[0];
    const image = mainContainer.image || '';
    const lowerImage = image.toLowerCase();

    if (lowerImage.includes('minio')) {
      return this.createRoleInfo('MinIO', 'storage', 'accent', 'S3兼容存储');
    }
    if (lowerImage.includes('sftp') || lowerImage.includes('openssh')) {
      return this.createRoleInfo('SFTP', 'service', 'basic', 'SFTP文件服务');
    }
    if (lowerImage.includes('mysql') || lowerImage.includes('xstore')) {
      return this.createRoleInfo('Database', 'storage', 'accent', '数据库');
    }
    if (lowerImage.includes('prometheus')) {
      return this.createRoleInfo('Prometheus', 'monitor', 'basic', '监控系统');
    }
    if (lowerImage.includes('grafana')) {
      return this.createRoleInfo('Grafana', 'monitor', 'basic', '可视化面板');
    }

    return null;
  }

  /**
   * 创建角色信息对象
   */
  private static createRoleInfo(
    role: string, 
    category: PodRoleInfo['category'], 
    color: PodRoleInfo['color'] = 'basic',
    description: string = ''
  ): PodRoleInfo {
    return {
      role,
      category,
      color,
      description: description || role
    };
  }

  /**
   * 获取角色颜色（兼容旧版API）
   */
  static getRoleColor(roleInfo: PodRoleInfo): string {
    switch (roleInfo.color) {
      case 'primary': return 'primary';
      case 'accent': return 'accent';
      case 'warn': return 'warn';
      case 'basic': return 'basic';
      default: return '';
    }
  }

  /**
   * 获取状态颜色
   */
  static getStatusColor(status: string): string {
    switch (status?.toLowerCase()) {
      case 'running': return 'primary';
      case 'pending': return 'warn';
      case 'succeeded': return 'accent';
      case 'failed': 
      case 'error': return 'warn';
      default: return 'basic';
    }
  }
}
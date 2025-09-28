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

    // 策略1: 从Pod标签获取角色（最准确）
    const labelRole = this.detectFromLabels(pod);
    if (labelRole) {
      return labelRole;
    }

    // 策略2: 从Pod名称模式推断
    const nameRole = this.detectFromName(pod.metadata.name || '');
    if (nameRole) {
      return nameRole;
    }

    // 策略3: 从容器镜像推断
    const imageRole = this.detectFromImage(pod);
    if (imageRole) {
      return imageRole;
    }

    // 默认：未知角色
    return this.createRoleInfo('Unknown', 'unknown');
  }

  /**
   * 从Pod标签检测角色（优先级最高）
   */
  private static detectFromLabels(pod: Pod): PodRoleInfo | null {
    const labels = pod.metadata?.labels || {};
    
    // PolarDB-X标准标签
    if (labels['polardbx/role']) {
      const role = labels['polardbx/role'].toUpperCase();
      switch (role) {
        case 'CN': return this.createRoleInfo('CN', 'compute', 'primary', '计算节点');
        case 'DN': return this.createRoleInfo('DN', 'storage', 'accent', '数据节点');
        case 'GMS': return this.createRoleInfo('GMS', 'service', 'warn', '全局元服务');
        case 'CDC': return this.createRoleInfo('CDC', 'service', 'basic', '变更数据捕获');
        case 'COLUMNAR': return this.createRoleInfo('Columnar', 'storage', 'accent', '列存储');
      }
    }

    // 节点角色标签
    if (labels['node-role']) {
      return this.createRoleInfo(labels['node-role'].toUpperCase(), 'compute');
    }

    // 应用标签
    if (labels['app']) {
      const app = labels['app'].toLowerCase();
      if (app.includes('minio')) {
        return this.createRoleInfo('MinIO', 'storage', 'accent', 'S3兼容存储');
      }
      if (app.includes('sftp')) {
        return this.createRoleInfo('SFTP', 'service', 'basic', 'SFTP文件服务');
      }
    }

    return null;
  }

  /**
   * 从Pod名称模式检测角色
   */
  private static detectFromName(name: string): PodRoleInfo | null {
    if (!name) return null;

    const lowerName = name.toLowerCase();

    // PolarDB-X组件模式
    if (lowerName.includes('-cn-')) {
      return this.createRoleInfo('CN', 'compute', 'primary', '计算节点');
    }
    if (lowerName.includes('-dn-')) {
      return this.createRoleInfo('DN', 'storage', 'accent', '数据节点');
    }
    if (lowerName.includes('-gms-')) {
      return this.createRoleInfo('GMS', 'service', 'warn', '全局元服务');
    }
    if (lowerName.includes('-cdc-')) {
      return this.createRoleInfo('CDC', 'service', 'basic', '变更数据捕获');
    }
    if (lowerName.includes('-columnar-')) {
      return this.createRoleInfo('Columnar', 'storage', 'accent', '列存储');
    }

    // 基础设施组件模式
    if (lowerName.startsWith('minio') || lowerName.includes('minio')) {
      return this.createRoleInfo('MinIO', 'storage', 'accent', 'S3兼容存储');
    }
    if (lowerName.startsWith('sftp') || lowerName.includes('sftp')) {
      return this.createRoleInfo('SFTP', 'service', 'basic', 'SFTP文件服务');
    }
    if (lowerName.includes('hpfs')) {
      return this.createRoleInfo('HPFS', 'service', 'basic', '高性能文件服务');
    }
    if (lowerName.includes('prometheus')) {
      return this.createRoleInfo('Prometheus', 'monitor', 'basic', '监控系统');
    }
    if (lowerName.includes('grafana')) {
      return this.createRoleInfo('Grafana', 'monitor', 'basic', '可视化面板');
    }
    if (lowerName.includes('alertmanager')) {
      return this.createRoleInfo('AlertManager', 'monitor', 'basic', '告警管理');
    }

    return null;
  }

  /**
   * 从容器镜像检测角色
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
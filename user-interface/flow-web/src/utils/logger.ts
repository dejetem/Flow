/**
 * Logger utility for conditional logging based on debug flag
 */

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

interface LoggerConfig {
  enabled: boolean;
  minLevel: LogLevel;
  prefix?: string;
}

class Logger {
  private config: LoggerConfig;
  
  private readonly levelPriority: Record<LogLevel, number> = {
    debug: 0,
    info: 1,
    warn: 2,
    error: 3,
  };

  constructor(config?: Partial<LoggerConfig>) {
    // Check for debug flag in multiple places (priority order)
    const isDebugEnabled =  import.meta.env.VITE_DEBUG === 'true' || import.meta.env.DEV; // Development mode

    this.config = {
      enabled: config?.enabled ?? isDebugEnabled,
      minLevel: config?.minLevel ?? 'debug',
      prefix: config?.prefix ?? '[Flow]',
    };
  }

  /**
   * Check if a log level should be logged
   */
  private shouldLog(level: LogLevel): boolean {
    if (!this.config.enabled) return false;
    return this.levelPriority[level] >= this.levelPriority[this.config.minLevel];
  }

  /**
   * Format the log message with prefix and timestamp
   */
  private formatMessage(level: LogLevel, ...args: any[]): any[] {
    const timestamp = new Date().toISOString().split('T')[1].slice(0, -1); // HH:MM:SS.mmm
    const prefix = `${this.config.prefix} [${level.toUpperCase()}] ${timestamp}`;
    return [prefix, ...args];
  }

  /**
   * Debug level logging
   */
  debug(...args: any[]): void {
    if (this.shouldLog('debug')) {
      console.debug(...this.formatMessage('debug', ...args));
    }
  }

  /**
   * Info level logging
   */
  info(...args: any[]): void {
    if (this.shouldLog('info')) {
      console.info(...this.formatMessage('info', ...args));
    }
  }

  /**
   * Warning level logging
   */
  warn(...args: any[]): void {
    if (this.shouldLog('warn')) {
      console.warn(...this.formatMessage('warn', ...args));
    }
  }

  /**
   * Error level logging (always logs even if debug is disabled)
   */
  error(...args: any[]): void {
    if (this.shouldLog('error')) {
      console.error(...this.formatMessage('error', ...args));
    }
  }

  /**
   * Log a group of related messages
   */
  group(label: string, level: LogLevel = 'debug'): void {
    if (this.shouldLog(level)) {
      console.group(...this.formatMessage(level, label));
    }
  }

  groupEnd(): void {
    if (this.config.enabled) {
      console.groupEnd();
    }
  }

  /**
   * Log an object in a formatted way
   */
  object(label: string, obj: any, level: LogLevel = 'debug'): void {
    if (this.shouldLog(level)) {
      console.log(...this.formatMessage(level, label));
      console.dir(obj, { depth: null, colors: true });
    }
  }

  /**
   * Time a function execution
   */
  time(label: string): void {
    if (this.config.enabled) {
      console.time(`${this.config.prefix} ${label}`);
    }
  }

  timeEnd(label: string): void {
    if (this.config.enabled) {
      console.timeEnd(`${this.config.prefix} ${label}`);
    }
  }

  /**
   * Enable/disable logging at runtime
   */
  setEnabled(enabled: boolean): void {
    this.config.enabled = enabled;
    localStorage.setItem('FLOW_DEBUG', String(enabled));
  }

  /**
   * Check if logging is enabled
   */
  isEnabled(): boolean {
    return this.config.enabled;
  }

  /**
   * Set minimum log level
   */
  setMinLevel(level: LogLevel): void {
    this.config.minLevel = level;
  }
}

// Export singleton instance
export const logger = new Logger();

// Export class for custom instances
export { Logger, type LogLevel, type LoggerConfig };

package org.example;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.ConfigOptions;

import java.util.Arrays;
import java.util.Map;

/**
 * Simplified Fluss Gateway - serves as Py4J entry point
 * Provides minimal Java-side interface for core object access
 */
public class FlussGateway {
    
    /**
     * 创建 Fluss 连接 - 简化版本，使用默认配置
     * @param bootstrapServers 引导服务器地址，例如 "localhost:9123"
     * @return FlussConnection 包装器
     */
    public FlussConnection createConnection(String bootstrapServers) {
        try {
            Configuration conf = new Configuration();
            
            // 设置 bootstrap servers
            conf.set(ConfigOptions.BOOTSTRAP_SERVERS,
                Arrays.asList(bootstrapServers.split(",")));
            
            Connection connection = ConnectionFactory.createConnection(conf);
            return new FlussConnection(connection);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to create connection: " + e.getMessage(), e);
        }
    }
    
    /**
     * 创建 Fluss 连接 - 带配置版本
     * @param configMap 配置映射
     * @return FlussConnection 包装器
     */
    public FlussConnection createConnection(Map<String, String> configMap) {
        try {
            Configuration conf = new Configuration();
            
            // 设置配置
            for (Map.Entry<String, String> entry : configMap.entrySet()) {
                conf.setString(entry.getKey(), entry.getValue());
            }
            
            // 设置 bootstrap servers
            if (configMap.containsKey("bootstrap.servers")) {
                conf.set(ConfigOptions.BOOTSTRAP_SERVERS,
                    Arrays.asList(configMap.get("bootstrap.servers").split(",")));
            }
            
            Connection connection = ConnectionFactory.createConnection(conf);
            return new FlussConnection(connection);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to create connection: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取 Schema 工具类
     * @return SchemaUtil 实例
     */
    public SchemaUtil getSchemaUtil() {
        return new SchemaUtil();
    }
    
    /**
     * 获取数据写入器工厂
     * @return FlussDataWriter 工厂
     */
    public FlussDataWriterFactory getDataWriterFactory() {
        return new FlussDataWriterFactory();
    }
    
    /**
     * 创建数据写入器
     * @param table 表实例
     * @return FlussDataWriter 实例
     */
    public FlussDataWriter createDataWriter(com.alibaba.fluss.client.table.Table table) {
        return new FlussDataWriter(table);
    }
    
    /**
     * 获取数据读取器工厂
     * @return FlussDataReaderFactory 实例
     */
    public FlussDataReaderFactory getDataReaderFactory() {
        return new FlussDataReaderFactory();
    }
}

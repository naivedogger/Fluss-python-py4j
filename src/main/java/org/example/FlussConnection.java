package org.example;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.metadata.TablePath;

/**
 * Simplified connection wrapper - exposes only necessary Fluss objects
 * No business logic, focuses on object access only
 */
public class FlussConnection {
    private final Connection connection;
    private final Admin admin;
    
    public FlussConnection(Connection connection) {
        this.connection = connection;
        this.admin = connection.getAdmin();
    }
    
    /**
     * 获取 Admin 客户端（Python 端负责具体操作）
     */
    public Admin getAdmin() {
        return admin;
    }
    
    /**
     * 获取 Table 实例（Python 端负责具体操作）
     */
    public Table getTable(String database, String tableName) {
        TablePath tablePath = TablePath.of(database, tableName);
        return connection.getTable(tablePath);
    }
    
    /**
     * 获取原始连接对象（高级用法）
     */
    public Connection getRawConnection() {
        return connection;
    }
    
    /**
     * 关闭连接
     */
    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to close connection: " + e.getMessage(), e);
        }
    }
}

package org.example;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertResult;
import com.alibaba.fluss.client.table.writer.AppendResult;
import com.alibaba.fluss.client.table.writer.DeleteResult;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * FlussDataWriter 专门处理Fluss数据写入操作
 * 支持Upsert和Append两种写入模式
 * 提供连接池管理和错误处理
 */
public class FlussDataWriter {
    
    private final Map<String, UpsertWriter> upsertWriterCache;
    private final Map<String, AppendWriter> appendWriterCache;
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    
    public FlussDataWriter() {
        this.upsertWriterCache = new HashMap<>();
        this.appendWriterCache = new HashMap<>();
    }
    
    /**
     * 获取或创建UpsertWriter
     * @param table Fluss表实例
     * @param database 数据库名称
     * @param tableName 表名称
     * @return UpsertWriter实例
     */
    public UpsertWriter getOrCreateUpsertWriter(Table table, String database, String tableName) {
        String writerKey = database + "." + tableName + ".upsert";
        
        if (upsertWriterCache.containsKey(writerKey)) {
            return upsertWriterCache.get(writerKey);
        }
        
        try {
            // 使用Fluss API创建UpsertWriter
            UpsertWriter writer = table.newUpsert().createWriter();
            upsertWriterCache.put(writerKey, writer);
            
            System.out.println("创建UpsertWriter成功: " + writerKey);
            return writer;
            
        } catch (Exception e) {
            System.err.println("创建UpsertWriter失败: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * 获取或创建AppendWriter
     * @param table Fluss表实例
     * @param database 数据库名称
     * @param tableName 表名称
     * @return AppendWriter实例
     */
    public AppendWriter getOrCreateAppendWriter(Table table, String database, String tableName) {
        String writerKey = database + "." + tableName + ".append";
        
        if (appendWriterCache.containsKey(writerKey)) {
            return appendWriterCache.get(writerKey);
        }
        
        try {
            // 使用Fluss API创建AppendWriter
            AppendWriter writer = table.newAppend().createWriter();
            appendWriterCache.put(writerKey, writer);
            
            System.out.println("创建AppendWriter成功: " + writerKey);
            return writer;
            
        } catch (Exception e) {
            System.err.println("创建AppendWriter失败: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * 批量写入数据到主键表（Upsert操作）
     * @param table Fluss表实例
     * @param database 数据库名称
     * @param tableName 表名称
     * @param records 记录列表
     * @return 写入是否成功
     */
    public boolean batchUpsert(Table table, String database, String tableName, List<Map<String, Object>> records) {
        UpsertWriter writer = getOrCreateUpsertWriter(table, database, tableName);
        if (writer == null) {
            System.err.println("无法获取UpsertWriter");
            return false;
        }
        
        try {
            TableInfo tableInfo = table.getTableInfo();
            RowType rowType = tableInfo.getRowType();
            
            // 批量写入
            for (Map<String, Object> record : records) {
                InternalRow row = convertMapToInternalRow(record, rowType);
                CompletableFuture<UpsertResult> future = writer.upsert(row);
                
                // 等待写入完成
                UpsertResult result = future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (result == null) {
                    System.err.println("Upsert操作失败: result为null");
                    return false;
                }
            }
            
            // 刷新写入器
            writer.flush();
            
            System.out.println("批量Upsert成功，共写入 " + records.size() + " 条记录");
            return true;
            
        } catch (Exception e) {
            System.err.println("批量Upsert失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 批量写入数据到日志表（Append操作）
     * @param table Fluss表实例
     * @param database 数据库名称
     * @param tableName 表名称
     * @param records 记录列表
     * @return 写入是否成功
     */
    public boolean batchAppend(Table table, String database, String tableName, List<Map<String, Object>> records) {
        AppendWriter writer = getOrCreateAppendWriter(table, database, tableName);
        if (writer == null) {
            System.err.println("无法获取AppendWriter");
            return false;
        }
        
        try {
            TableInfo tableInfo = table.getTableInfo();
            RowType rowType = tableInfo.getRowType();
            
            // 批量写入
            for (Map<String, Object> record : records) {
                InternalRow row = convertMapToInternalRow(record, rowType);
                CompletableFuture<AppendResult> future = writer.append(row);
                
                // 等待写入完成
                AppendResult result = future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (result == null) {
                    System.err.println("Append操作失败: result为null");
                    return false;
                }
            }
            
            // 刷新写入器
            writer.flush();
            
            System.out.println("批量Append成功，共写入 " + records.size() + " 条记录");
            return true;
            
        } catch (Exception e) {
            System.err.println("批量Append失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 单条记录Upsert操作
     * @param table Fluss表实例
     * @param database 数据库名称
     * @param tableName 表名称
     * @param record 记录
     * @return 写入是否成功
     */
    public boolean upsertSingle(Table table, String database, String tableName, Map<String, Object> record) {
        UpsertWriter writer = getOrCreateUpsertWriter(table, database, tableName);
        if (writer == null) {
            return false;
        }
        
        try {
            TableInfo tableInfo = table.getTableInfo();
            RowType rowType = tableInfo.getRowType();
            
            InternalRow row = convertMapToInternalRow(record, rowType);
            CompletableFuture<UpsertResult> future = writer.upsert(row);
            
            UpsertResult result = future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (result == null) {
                System.err.println("Upsert操作失败: result为null");
                return false;
            }
            
            writer.flush();
            return true;
            
        } catch (Exception e) {
            System.err.println("Upsert操作失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 单条记录Append操作
     * @param table Fluss表实例
     * @param database 数据库名称
     * @param tableName 表名称
     * @param record 记录
     * @return 写入是否成功
     */
    public boolean appendSingle(Table table, String database, String tableName, Map<String, Object> record) {
        AppendWriter writer = getOrCreateAppendWriter(table, database, tableName);
        if (writer == null) {
            return false;
        }
        
        try {
            TableInfo tableInfo = table.getTableInfo();
            RowType rowType = tableInfo.getRowType();
            
            InternalRow row = convertMapToInternalRow(record, rowType);
            CompletableFuture<AppendResult> future = writer.append(row);
            
            AppendResult result = future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (result == null) {
                System.err.println("Append操作失败: result为null");
                return false;
            }
            
            writer.flush();
            return true;
            
        } catch (Exception e) {
            System.err.println("Append操作失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 删除记录（主键表）
     * @param table Fluss表实例
     * @param database 数据库名称
     * @param tableName 表名称
     * @param record 包含主键信息的记录
     * @return 删除是否成功
     */
    public boolean deleteRecord(Table table, String database, String tableName, Map<String, Object> record) {
        UpsertWriter writer = getOrCreateUpsertWriter(table, database, tableName);
        if (writer == null) {
            return false;
        }
        
        try {
            TableInfo tableInfo = table.getTableInfo();
            RowType rowType = tableInfo.getRowType();
            
            InternalRow row = convertMapToInternalRow(record, rowType);
            CompletableFuture<DeleteResult> future = writer.delete(row);
            
            DeleteResult result = future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (result == null) {
                System.err.println("Delete操作失败: result为null");
                return false;
            }
            
            writer.flush();
            return true;
            
        } catch (Exception e) {
            System.err.println("Delete操作失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 转换Map为InternalRow
     * @param record 记录Map
     * @param rowType 行类型
     * @return InternalRow实例
     */
    private InternalRow convertMapToInternalRow(Map<String, Object> record, RowType rowType) {
        Object[] values = new Object[rowType.getFieldCount()];
        
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            Object value = record.get(fieldName);
            
            // 根据字段类型转换值
            if (value != null) {
                DataType fieldType = rowType.getTypeAt(i);
                values[i] = convertValue(value, fieldType);
            } else {
                values[i] = null;
            }
        }
        
        return GenericRow.of(values);
    }
    
    /**
     * 转换值为指定类型
     * @param value 原始值
     * @param dataType 目标数据类型
     * @return 转换后的值
     */
    private Object convertValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        
        try {
            switch (dataType.getTypeRoot()) {
                case BOOLEAN:
                    return value instanceof Boolean ? value : Boolean.parseBoolean(value.toString());
                case TINYINT:
                    return value instanceof Byte ? value : Byte.parseByte(value.toString());
                case SMALLINT:
                    return value instanceof Short ? value : Short.parseShort(value.toString());
                case INTEGER:
                    return value instanceof Integer ? value : Integer.parseInt(value.toString());
                case BIGINT:
                    return value instanceof Long ? value : Long.parseLong(value.toString());
                case FLOAT:
                    return value instanceof Float ? value : Float.parseFloat(value.toString());
                case DOUBLE:
                    return value instanceof Double ? value : Double.parseDouble(value.toString());
                case STRING:
                case CHAR:
                    return BinaryString.fromString(value.toString());
                case DATE:
                    if (value instanceof java.sql.Date) {
                        return value;
                    } else if (value instanceof String) {
                        return java.sql.Date.valueOf(value.toString());
                    } else {
                        return java.sql.Date.valueOf(value.toString());
                    }
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    if (value instanceof java.sql.Timestamp) {
                        java.sql.Timestamp ts = (java.sql.Timestamp) value;
                        return TimestampNtz.fromMillis(ts.getTime());
                    } else if (value instanceof String) {
                        java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.toString());
                        return TimestampNtz.fromMillis(ts.getTime());
                    } else {
                        java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.toString());
                        return TimestampNtz.fromMillis(ts.getTime());
                    }
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    // 处理带本地时区的时间戳
                    if (value instanceof java.sql.Timestamp) {
                        java.sql.Timestamp ts = (java.sql.Timestamp) value;
                        return TimestampNtz.fromMillis(ts.getTime());
                    } else if (value instanceof String) {
                        java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.toString());
                        return TimestampNtz.fromMillis(ts.getTime());
                    } else {
                        java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.toString());
                        return TimestampNtz.fromMillis(ts.getTime());
                    }
                case DECIMAL:
                    if (value instanceof java.math.BigDecimal) {
                        return value;
                    } else {
                        return new java.math.BigDecimal(value.toString());
                    }
                default:
                    return value.toString();
            }
        } catch (Exception e) {
            System.err.println("转换值失败: " + e.getMessage() + ", 值: " + value + ", 类型: " + dataType);
            return value.toString();
        }
    }
    
    /**
     * 刷新所有Writer
     */
    public void flushAll() {
        try {
            for (UpsertWriter writer : upsertWriterCache.values()) {
                if (writer != null) {
                    writer.flush();
                }
            }
            for (AppendWriter writer : appendWriterCache.values()) {
                if (writer != null) {
                    writer.flush();
                }
            }
            System.out.println("所有Writer刷新完成");
        } catch (Exception e) {
            System.err.println("刷新Writer失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 清理所有缓存的Writer
     */
    public void closeAll() {
        try {
            // UpsertWriter和AppendWriter都没有close方法，只需要清理缓存
            upsertWriterCache.clear();
            appendWriterCache.clear();
            System.out.println("所有Writer已清理");
        } catch (Exception e) {
            System.err.println("清理Writer失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 获取已缓存的Writer数量
     * @return Writer数量信息
     */
    public Map<String, Integer> getWriterStats() {
        Map<String, Integer> stats = new HashMap<>();
        stats.put("upsertWriters", upsertWriterCache.size());
        stats.put("appendWriters", appendWriterCache.size());
        return stats;
    }
}

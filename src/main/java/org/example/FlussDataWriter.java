package org.example;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.DataType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.ArrayList;

/**
 * 数据写入器 - 提供实际的数据写入功能
 */
public class FlussDataWriter {
    
    private final Table table;
    private final RowType rowType;
    
    public FlussDataWriter(Table table) {
        this.table = table;
        this.rowType = table.getTableInfo().getRowType();
    }
    
    /**
     * 创建一行数据（GenericRow）
     * @param values 字段值数组
     * @return GenericRow
     */
    public GenericRow createRow(Object[] values) {
        try {
            GenericRow row = new GenericRow(values.length);
            
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                
                if (value != null) {
                    DataType fieldType = rowType.getChildren().get(i);
                    Object convertedValue = convertValueToFlussType(value, fieldType);
                    row.setField(i, convertedValue);
                } else {
                    row.setField(i, null);
                }
            }
            
            return row;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create row: " + e.getMessage(), e);
        }
    }
    
    /**
     * 使用UpsertWriter写入数据
     * @param data 数据行列表，每行是Object[]
     * @return 写入的记录数
     */
    public int writeDataWithUpsert(List<Object[]> data) {
        UpsertWriter writer = null;
        int writeCount = 0;
        
        try {
            writer = table.newUpsert().createWriter();
            
            for (Object[] rowValues : data) {
                GenericRow row = createRow(rowValues);
                writer.upsert(row);
                writeCount++;
            }
            
            writer.flush();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to write data with upsert: " + e.getMessage(), e);
        } finally {
            if (writer != null) {
                try {
                    // UpsertWriter可能没有close方法，直接flush即可
                } catch (Exception e) {
                    System.err.println("Error closing upsert writer: " + e.getMessage());
                }
            }
        }
        
        return writeCount;
    }
    
    /**
     * 使用AppendWriter写入数据
     * @param data 数据行列表，每行是Object[]
     * @return 写入的记录数
     */
    public int writeDataWithAppend(List<Object[]> data) {
        AppendWriter writer = null;
        int writeCount = 0;
        
        try {
            writer = table.newAppend().createWriter();
            
            for (Object[] rowValues : data) {
                GenericRow row = createRow(rowValues);
                writer.append(row);
                writeCount++;
            }
            
            writer.flush();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to write data with append: " + e.getMessage(), e);
        } finally {
            if (writer != null) {
                try {
                    // AppendWriter可能没有close方法，直接flush即可
                } catch (Exception e) {
                    System.err.println("Error closing append writer: " + e.getMessage());
                }
            }
        }
        
        return writeCount;
    }
    
    /**
     * 写入示例PK表数据
     * @return 写入的记录数
     */
    public int writeSamplePKData() {
        List<Object[]> sampleData = new ArrayList<>();
        
        sampleData.add(new Object[]{1, "Alice", "100"});
        sampleData.add(new Object[]{2, "Bob", "200"});
        sampleData.add(new Object[]{3, "Charlie", "300"});
        sampleData.add(new Object[]{1, "Alice Updated", "150"});
        
        return writeDataWithUpsert(sampleData);
    }
    
    /**
     * 写入示例Log表数据
     * @return 写入的记录数
     */
    public int writeSampleLogData() {
        List<Object[]> sampleData = new ArrayList<>();
        
        long currentTime = System.currentTimeMillis();
        
        sampleData.add(new Object[]{Long.valueOf(currentTime), "Application started", "INFO"});
        sampleData.add(new Object[]{Long.valueOf(currentTime + 1000L), "User login", "INFO"});
        sampleData.add(new Object[]{Long.valueOf(currentTime + 2000L), "Database connection error", "ERROR"});
        sampleData.add(new Object[]{Long.valueOf(currentTime + 3000L), "Operation completed", "INFO"});
        
        return writeDataWithAppend(sampleData);
    }
    
    /**
     * 将Python传入的值转换为Fluss类型
     * @param value 原始值
     * @param dataType Fluss数据类型
     * @return 转换后的值
     */
    private Object convertValueToFlussType(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        
        String typeStr = dataType.toString().toLowerCase();
        
        try {
            if (typeStr.contains("int") && !typeStr.contains("bigint")) {
                // 处理INT类型，排除BIGINT
                if (value instanceof Integer) {
                    return value;
                } else if (value instanceof Number) {
                    return Integer.valueOf(((Number) value).intValue());
                } else {
                    return Integer.valueOf(value.toString());
                }
            } else if (typeStr.contains("bigint")) {
                // 强制转换为Long类型，确保类型正确
                if (value instanceof Long) {
                    return value;
                } else if (value instanceof Number) {
                    return Long.valueOf(((Number) value).longValue());
                } else {
                    return Long.valueOf(value.toString());
                }
            } else if (typeStr.contains("string") || typeStr.contains("varchar")) {
                // 转换为BinaryString
                return com.alibaba.fluss.row.BinaryString.fromString(value.toString());
            } else if (typeStr.contains("double")) {
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else {
                    return Double.parseDouble(value.toString());
                }
            } else if (typeStr.contains("float")) {
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                } else {
                    return Float.parseFloat(value.toString());
                }
            } else if (typeStr.contains("boolean")) {
                if (value instanceof Boolean) {
                    return value;
                } else {
                    return Boolean.parseBoolean(value.toString());
                }
            } else if (typeStr.contains("timestamp")) {
                if (value instanceof Number) {
                    long timestamp = ((Number) value).longValue();
                    return Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC).toLocalDateTime();
                } else {
                    return LocalDateTime.parse(value.toString());
                }
            }
            
            return value;
            
        } catch (Exception e) {
            System.err.println("Type conversion warning: " + e.getMessage());
            return value;
        }
    }
    
    public RowType getRowType() {
        return rowType;
    }
    
    public Table getTable() {
        return table;
    }
}

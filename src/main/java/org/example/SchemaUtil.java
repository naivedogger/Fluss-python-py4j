package org.example;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.DataTypeRoot;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Fluss Schema工具类
 * 提供Schema相关的转换和解析功能
 */
public class SchemaUtil {
    
    /**
     * 将Fluss Schema转换为简化的字典格式
     * @param schema Fluss Schema对象
     * @return 包含字段信息的Map
     */
    public static Map<String, Object> schemaToMap(Schema schema) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // 获取行类型 - 使用正确的API方法
            RowType rowType = schema.getRowType();
            
            // 转换字段信息
            List<Map<String, Object>> fields = new ArrayList<>();
            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getChildren();
            
            for (int i = 0; i < fieldNames.size(); i++) {
                Map<String, Object> fieldInfo = new HashMap<>();
                fieldInfo.put("name", fieldNames.get(i));
                fieldInfo.put("type", dataTypeToString(fieldTypes.get(i)));
                fieldInfo.put("raw_type", fieldTypes.get(i).toString());
                fieldInfo.put("nullable", fieldTypes.get(i).isNullable());
                fieldInfo.put("index", i);
                fields.add(fieldInfo);
            }
            
            result.put("fields", fields);
            result.put("field_count", fieldNames.size());
            result.put("field_names", fieldNames);
            
            // 获取主键信息
            if (schema.getPrimaryKey().isPresent()) {
                result.put("primary_key", schema.getPrimaryKey().get().getColumnNames());
            }
            
            // 获取分区键信息 - 需要检查实际API
            // if (schema.getPartitionKeys().isPresent()) {
            //     result.put("partition_keys", schema.getPartitionKeys().get());
            // }
            
        } catch (Exception e) {
            result.put("error", "Schema解析失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        return result;
    }
    
    /**
     * 将DataType转换为Python友好的字符串格式
     * @param dataType Fluss DataType
     * @return 类型字符串
     */
    public static String dataTypeToString(DataType dataType) {
        if (dataType == null) {
            return "UNKNOWN";
        }
        
        // 简化类型名称，便于Python端处理
        String typeStr = dataType.toString().toLowerCase();
        
        // 常见类型映射
        if (typeStr.contains("int")) {
            return "INTEGER";
        } else if (typeStr.contains("bigint")) {
            return "BIGINT";
        } else if (typeStr.contains("string") || typeStr.contains("varchar")) {
            return "STRING";
        } else if (typeStr.contains("double")) {
            return "DOUBLE";
        } else if (typeStr.contains("float")) {
            return "FLOAT";
        } else if (typeStr.contains("boolean")) {
            return "BOOLEAN";
        } else if (typeStr.contains("timestamp")) {
            return "TIMESTAMP";
        } else if (typeStr.contains("date")) {
            return "DATE";
        } else if (typeStr.contains("decimal")) {
            return "DECIMAL";
        } else if (typeStr.contains("bytes") || typeStr.contains("binary")) {
            return "BYTES";
        }
        
        return typeStr.toUpperCase();
    }
    
    /**
     * 获取字段的详细信息
     * @param schema Fluss Schema
     * @param fieldName 字段名
     * @return 字段详细信息
     */
    public static Map<String, Object> getFieldInfo(Schema schema, String fieldName) {
        Map<String, Object> fieldInfo = new HashMap<>();
        
        try {
            RowType rowType = schema.getRowType();
            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getChildren();
            
            int fieldIndex = fieldNames.indexOf(fieldName);
            if (fieldIndex >= 0) {
                DataType fieldType = fieldTypes.get(fieldIndex);
                fieldInfo.put("name", fieldName);
                fieldInfo.put("index", fieldIndex);
                fieldInfo.put("type", dataTypeToString(fieldType));
                fieldInfo.put("raw_type", fieldType.toString());
                fieldInfo.put("nullable", fieldType.isNullable());
                fieldInfo.put("exists", true);
            } else {
                fieldInfo.put("exists", false);
                fieldInfo.put("error", "字段不存在: " + fieldName);
            }
            
        } catch (Exception e) {
            fieldInfo.put("exists", false);
            fieldInfo.put("error", "获取字段信息失败: " + e.getMessage());
        }
        
        return fieldInfo;
    }
    
    /**
     * 验证Schema是否有效
     * @param schema Fluss Schema
     * @return 验证结果
     */
    public static Map<String, Object> validateSchema(Schema schema) {
        Map<String, Object> result = new HashMap<>();
        List<String> warnings = new ArrayList<>();
        
        try {
            RowType rowType = schema.getRowType();
            List<String> fieldNames = rowType.getFieldNames();
            
            // 基本验证
            if (fieldNames.isEmpty()) {
                warnings.add("Schema没有字段");
            }
            
            // 检查重复字段名
            for (int i = 0; i < fieldNames.size(); i++) {
                for (int j = i + 1; j < fieldNames.size(); j++) {
                    if (fieldNames.get(i).equals(fieldNames.get(j))) {
                        warnings.add("发现重复字段名: " + fieldNames.get(i));
                    }
                }
            }
            
            result.put("valid", warnings.isEmpty());
            result.put("warnings", warnings);
            result.put("field_count", fieldNames.size());
            
        } catch (Exception e) {
            result.put("valid", false);
            result.put("error", "Schema验证失败: " + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * 将InternalRow转换为字符串
     * @param row 内部行数据
     * @param rowType 行类型
     * @return 字符串表示
     */
    public static String convertRowToString(com.alibaba.fluss.row.InternalRow row, RowType rowType) {
        if (row == null) {
            return "null";
        }
        
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            
            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getChildren();
            
            for (int i = 0; i < fieldNames.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                
                sb.append("\"").append(fieldName).append("\": ");
                
                // 根据字段类型获取值
                Object value = getFieldValue(row, i, fieldType);
                if (value == null) {
                    sb.append("null");
                } else if (value instanceof String) {
                    sb.append("\"").append(value).append("\"");
                } else {
                    sb.append(value.toString());
                }
            }
            
            sb.append("}");
            return sb.toString();
            
        } catch (Exception e) {
            return "Error converting row: " + e.getMessage();
        }
    }
    
    /**
     * 根据字段类型获取字段值
     * @param row 行数据
     * @param index 字段索引
     * @param dataType 数据类型
     * @return 字段值
     */
    private static Object getFieldValue(com.alibaba.fluss.row.InternalRow row, int index, DataType dataType) {
        try {
            if (row.isNullAt(index)) {
                return null;
            }
            
            // 使用简化的值获取方法
            DataTypeRoot typeRoot = dataType.getTypeRoot();
            
            switch (typeRoot) {
                case INTEGER:
                    return row.getInt(index);
                case BIGINT:
                    return row.getLong(index);
                case CHAR:
                case STRING:
                    return row.getString(index).toString();
                case BOOLEAN:
                    return row.getBoolean(index);
                case FLOAT:
                    return row.getFloat(index);
                case DOUBLE:
                    return row.getDouble(index);
                case DATE:
                    return row.getInt(index); // Date stored as int
                default:
                    // 对于其他复杂类型，尝试转为字符串
                    try {
                        return row.getString(index).toString();
                    } catch (Exception e2) {
                        return "Unsupported type: " + typeRoot;
                    }
            }
        } catch (Exception e) {
            return "Error reading field: " + e.getMessage();
        }
    }
    
    /**
     * 公共方法：获取字段值（供 Predicate 过滤使用）
     * @param row 内部行数据
     * @param index 字段索引
     * @param dataType 数据类型
     * @return 字段值
     */
    public static Object getFieldValuePublic(com.alibaba.fluss.row.InternalRow row, int index, DataType dataType) {
        return getFieldValue(row, index, dataType);
    }
}

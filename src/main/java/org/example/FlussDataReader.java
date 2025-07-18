package org.example;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.Scan;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fluss数据读取器
 * 提供表数据的读取功能，使用实际的Fluss API
 */
public class FlussDataReader {
    
    private final Table table;
    private final String readerType;
    private Scan scan;
    private BatchScanner batchScanner;
    private LogScanner logScanner;
    private boolean isInitialized = false;
    
    public FlussDataReader(Table table, String readerType) {
        this.table = table;
        this.readerType = readerType;
    }
    
    /**
     * 初始化读取器
     * @return 初始化是否成功
     */
    public boolean initialize() {
        try {
            // 创建扫描对象
            this.scan = table.newScan();
            
            // 根据读取器类型初始化不同的扫描器
            if ("snapshot".equalsIgnoreCase(readerType) || "batch".equalsIgnoreCase(readerType)) {
                // 创建批量扫描器，需要指定表桶和limit
                TableInfo tableInfo = table.getTableInfo();
                // 使用默认的表桶，对于非分区表使用桶0
                TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
                
                // 设置一个默认的limit（比如1000条记录）
                Scan limitedScan = scan.limit(1000);
                this.batchScanner = limitedScan.createBatchScanner(tableBucket);
                this.isInitialized = true;
                System.out.println("批量/快照读取器初始化成功");
                return true;
            } else if ("log".equalsIgnoreCase(readerType) || "stream".equalsIgnoreCase(readerType)) {
                // 创建日志扫描器
                this.logScanner = scan.createLogScanner();
                // 订阅从开始位置读取
                this.logScanner.subscribeFromBeginning(0);
                this.isInitialized = true;
                System.out.println("日志/流读取器初始化成功");
                return true;
            } else if ("lookup".equalsIgnoreCase(readerType)) {
                // 查找读取器使用批量扫描器
                TableInfo tableInfo = table.getTableInfo();
                TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
                
                // 设置一个默认的limit（比如1000条记录）
                Scan limitedScan = scan.limit(1000);
                this.batchScanner = limitedScan.createBatchScanner(tableBucket);
                this.isInitialized = true;
                System.out.println("查找读取器初始化成功");
                return true;
            } else {
                System.err.println("不支持的读取器类型: " + readerType);
                return false;
            }
        } catch (Exception e) {
            System.err.println("初始化读取器失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 初始化读取器（带limit参数）
     * @param limit 限制读取的记录数
     * @return 初始化是否成功
     */
    public boolean initialize(int limit) {
        try {
            // 创建扫描对象
            this.scan = table.newScan();
            
            // 根据读取器类型初始化不同的扫描器
            if ("snapshot".equalsIgnoreCase(readerType) || "batch".equalsIgnoreCase(readerType)) {
                // 创建批量扫描器，需要指定表桶和limit
                TableInfo tableInfo = table.getTableInfo();
                // 使用默认的表桶，对于非分区表使用桶0
                TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
                
                // 设置用户指定的limit
                Scan limitedScan = scan.limit(limit);
                this.batchScanner = limitedScan.createBatchScanner(tableBucket);
                this.isInitialized = true;
                System.out.println("批量/快照读取器初始化成功，limit=" + limit);
                return true;
            } else if ("log".equalsIgnoreCase(readerType) || "stream".equalsIgnoreCase(readerType)) {
                // 创建日志扫描器
                this.logScanner = scan.createLogScanner();
                // 订阅从开始位置读取
                this.logScanner.subscribeFromBeginning(0);
                this.isInitialized = true;
                System.out.println("日志/流读取器初始化成功");
                return true;
            } else if ("lookup".equalsIgnoreCase(readerType)) {
                // 查找读取器使用批量扫描器
                TableInfo tableInfo = table.getTableInfo();
                TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
                
                // 设置用户指定的limit
                Scan limitedScan = scan.limit(limit);
                this.batchScanner = limitedScan.createBatchScanner(tableBucket);
                this.isInitialized = true;
                System.out.println("查找读取器初始化成功，limit=" + limit);
                return true;
            } else {
                System.err.println("不支持的读取器类型: " + readerType);
                return false;
            }
        } catch (Exception e) {
            System.err.println("初始化读取器失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 读取数据记录
     * @param maxRecords 最大记录数
     * @return 记录列表
     */
    public List<Map<String, Object>> readRecords(int maxRecords) {
        List<Map<String, Object>> records = new ArrayList<>();
        
        if (!isInitialized) {
            System.err.println("读取器未初始化");
            return records;
        }
        
        try {
            System.out.println("开始读取数据，最大记录数: " + maxRecords);
            
            if (batchScanner != null) {
                // 使用批量扫描器读取数据
                records = readFromBatchScanner(maxRecords);
            } else if (logScanner != null) {
                // 使用日志扫描器读取数据
                records = readFromLogScanner(maxRecords);
            } else {
                System.err.println("没有可用的扫描器");
            }
            
        } catch (Exception e) {
            System.err.println("读取数据失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        return records;
    }
    
    /**
     * 从批量扫描器读取数据
     * @param maxRecords 最大记录数
     * @return 记录列表
     */
    private List<Map<String, Object>> readFromBatchScanner(int maxRecords) {
        List<Map<String, Object>> records = new ArrayList<>();
        
        try {
            // 设置读取超时时间
            Duration timeout = Duration.ofSeconds(10);
            
            // 轮询批量数据
            CloseableIterator<InternalRow> iterator = batchScanner.pollBatch(timeout);
            
            if (iterator != null) {
                int count = 0;
                while (iterator.hasNext() && count < maxRecords) {
                    InternalRow row = iterator.next();
                    Map<String, Object> record = convertInternalRowToMap(row, count);
                    records.add(record);
                    count++;
                }
                
                // 关闭迭代器
                iterator.close();
                System.out.println("批量读取完成，共读取 " + records.size() + " 条记录");
            } else {
                System.out.println("没有可用的批量数据");
            }
            
        } catch (IOException e) {
            System.err.println("批量扫描失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        return records;
    }
    
    /**
     * 从日志扫描器读取数据
     * @param maxRecords 最大记录数
     * @return 记录列表
     */
    private List<Map<String, Object>> readFromLogScanner(int maxRecords) {
        List<Map<String, Object>> records = new ArrayList<>();
        
        try {
            // 设置读取超时时间
            Duration timeout = Duration.ofSeconds(10);
            
            // 轮询日志数据
            ScanRecords scanRecords = logScanner.poll(timeout);
            
            if (scanRecords != null && !scanRecords.isEmpty()) {
                int count = 0;
                for (ScanRecord scanRecord : scanRecords) {
                    if (count >= maxRecords) break;
                    
                    Map<String, Object> record = convertScanRecordToMap(scanRecord, count);
                    records.add(record);
                    count++;
                }
                
                System.out.println("日志读取完成，共读取 " + records.size() + " 条记录");
            } else {
                System.out.println("没有可用的日志数据");
            }
            
        } catch (Exception e) {
            System.err.println("日志扫描失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        return records;
    }
    
    /**
     * 转换InternalRow为Map格式
     * @param row 内部行数据
     * @param index 记录索引
     * @return Map格式的记录
     */
    private Map<String, Object> convertInternalRowToMap(InternalRow row, int index) {
        Map<String, Object> record = new HashMap<>();
        
        try {
            // 获取表信息和字段数量
            TableInfo tableInfo = table.getTableInfo();
            RowType rowType = tableInfo.getRowType();
            
            record.put("_row_index", index);
            record.put("_field_count", row.getFieldCount());
            
            // 遍历所有字段
            for (int i = 0; i < row.getFieldCount(); i++) {
                String fieldName = "field_" + i;
                if (i < rowType.getFieldCount()) {
                    fieldName = rowType.getFieldNames().get(i);
                }
                
                Object value = extractFieldValue(row, i, rowType.getTypeAt(i));
                record.put(fieldName, value);
            }
            
        } catch (Exception e) {
            System.err.println("转换InternalRow时发生错误: " + e.getMessage());
            record.put("_error", "转换记录失败: " + e.getMessage());
            record.put("_row_index", index);
        }
        
        return record;
    }
    
    /**
     * 转换ScanRecord为Map格式
     * @param scanRecord 扫描记录
     * @param index 记录索引
     * @return Map格式的记录
     */
    private Map<String, Object> convertScanRecordToMap(ScanRecord scanRecord, int index) {
        Map<String, Object> record = new HashMap<>();
        
        try {
            // 添加元数据
            record.put("_log_offset", scanRecord.logOffset());
            record.put("_timestamp", scanRecord.timestamp());
            record.put("_change_type", scanRecord.getChangeType().name());
            record.put("_row_index", index);
            
            // 转换内部行数据
            InternalRow row = scanRecord.getRow();
            if (row != null) {
                Map<String, Object> rowData = convertInternalRowToMap(row, index);
                record.putAll(rowData);
            }
            
        } catch (Exception e) {
            System.err.println("转换ScanRecord时发生错误: " + e.getMessage());
            record.put("_error", "转换记录失败: " + e.getMessage());
            record.put("_row_index", index);
        }
        
        return record;
    }
    
    /**
     * 从InternalRow中提取字段值
     * @param row 内部行数据
     * @param fieldIndex 字段索引
     * @param dataType 数据类型
     * @return 字段值
     */
    private Object extractFieldValue(InternalRow row, int fieldIndex, DataType dataType) {
        try {
            if (row.isNullAt(fieldIndex)) {
                return null;
            }
            
            // 根据数据类型提取相应的值
            switch (dataType.getTypeRoot()) {
                case BOOLEAN:
                    return row.getBoolean(fieldIndex);
                case TINYINT:
                    return row.getByte(fieldIndex);
                case SMALLINT:
                    return row.getShort(fieldIndex);
                case INTEGER:
                    return row.getInt(fieldIndex);
                case BIGINT:
                    return row.getLong(fieldIndex);
                case FLOAT:
                    return row.getFloat(fieldIndex);
                case DOUBLE:
                    return row.getDouble(fieldIndex);
                case STRING:
                    return row.getString(fieldIndex).toString();
                case BYTES:
                    return row.getBytes(fieldIndex);
                default:
                    // 对于其他类型，尝试转换为字符串
                    return row.getString(fieldIndex) != null ? row.getString(fieldIndex).toString() : "N/A";
            }
            
        } catch (Exception e) {
            System.err.println("提取字段值失败: " + e.getMessage());
            return "ERROR: " + e.getMessage();
        }
    }
    
    /**
     * 获取读取器状态
     * @return 状态信息
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("reader_type", readerType);
        status.put("initialized", isInitialized);
        status.put("table_class", table.getClass().getSimpleName());
        status.put("has_batch_scanner", batchScanner != null);
        status.put("has_log_scanner", logScanner != null);
        
        try {
            TableInfo tableInfo = table.getTableInfo();
            status.put("table_id", tableInfo.getTableId());
            status.put("table_path", tableInfo.getTablePath().toString());
        } catch (Exception e) {
            status.put("table_info_error", e.getMessage());
        }
        
        return status;
    }
    
    /**
     * 关闭读取器
     */
    public void close() {
        try {
            if (batchScanner != null) {
                batchScanner.close();
                batchScanner = null;
            }
            
            if (logScanner != null) {
                logScanner.close();
                logScanner = null;
            }
            
            isInitialized = false;
            System.out.println("数据读取器已关闭");
        } catch (Exception e) {
            System.err.println("关闭读取器失败: " + e.getMessage());
        }
    }
}

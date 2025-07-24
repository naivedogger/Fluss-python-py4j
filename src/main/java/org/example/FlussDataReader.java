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
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Data reader - based on Fluss's data reading design
 * Only responsible for data reading access, no complex business logic
 */
public class FlussDataReader {
    
    private final Table table;
    private final String scanType;
    
    public FlussDataReader(Table table, String scanType) {
        this.table = table;
        this.scanType = scanType;
    }
    
    /**
     * 创建批量扫描器
     * @param limit 限制读取的记录数
     * @return BatchScanner
     */
    public BatchScanner createBatchScanner(int limit) {
        try {
            Scan scan = table.newScan().limit(limit);
            TableInfo tableInfo = table.getTableInfo();
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 0);
            return scan.createBatchScanner(tableBucket);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create batch scanner: " + e.getMessage(), e);
        }
    }
    
    /**
     * 创建日志扫描器
     * @return LogScanner
     */
    public LogScanner createLogScanner() {
        try {
            Scan scan = table.newScan();
            LogScanner logScanner = scan.createLogScanner();
            logScanner.subscribeFromBeginning(0);
            return logScanner;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create log scanner: " + e.getMessage(), e);
        }
    }
    
    /**
     * 读取批量数据并转换为字符串列表
     * @param limit 限制读取的记录数
     * @return 数据列表
     */
    public List<String> readBatchData(int limit) {
        List<String> results = new ArrayList<>();
        BatchScanner scanner = null;
        
        try {
            scanner = createBatchScanner(limit);
            Duration timeout = Duration.ofSeconds(10);
            CloseableIterator<InternalRow> iterator = scanner.pollBatch(timeout);
            
            if (iterator != null) {
                while (iterator.hasNext() && results.size() < limit) {
                    InternalRow row = iterator.next();
                    String rowData = convertRowToString(row);
                    results.add(rowData);
                }
                iterator.close();
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to read batch data: " + e.getMessage(), e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException e) {
                    // Log but don't throw
                    System.err.println("Error closing scanner: " + e.getMessage());
                }
            }
        }
        
        return results;
    }
    
    /**
     * 读取流数据
     * @param timeout 超时时间（毫秒）
     * @param maxRecords 最大记录数
     * @return 数据列表
     */
    public List<String> readStreamData(long timeout, int maxRecords) {
        List<String> results = new ArrayList<>();
        LogScanner scanner = null;
        
        try {
            scanner = createLogScanner();
            Duration timeoutDuration = Duration.ofMillis(timeout);
            ScanRecords scanRecords = scanner.poll(timeoutDuration);
            
            if (scanRecords != null) {
                // 使用正确的API
                for (ScanRecord record : scanRecords) {
                    if (results.size() >= maxRecords) {
                        break;
                    }
                    InternalRow row = record.getRow();
                    String rowData = convertRowToString(row);
                    results.add(rowData);
                }
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to read stream data: " + e.getMessage(), e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (Exception e) {
                    // Log but don't throw
                    System.err.println("Error closing stream scanner: " + e.getMessage());
                }
            }
        }
        
        return results;
    }
    
    /**
     * 获取表模式信息
     * @return RowType
     */
    public RowType getTableSchema() {
        try {
            return table.getTableInfo().getRowType();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get table schema: " + e.getMessage(), e);
        }
    }
    
    /**
     * 将行数据转换为字符串
     * @param row 内部行数据
     * @return 字符串表示
     */
    private String convertRowToString(InternalRow row) {
        if (row == null) {
            return "null";
        }
        
        try {
            RowType rowType = getTableSchema();
            return SchemaUtil.convertRowToString(row, rowType);
        } catch (Exception e) {
            return "Error converting row: " + e.getMessage();
        }
    }
    
    /**
     * 获取表信息
     * @return Table
     */
    public Table getTable() {
        return table;
    }
    
    /**
     * 获取扫描类型
     * @return 扫描类型
     */
    public String getScanType() {
        return scanType;
    }
}

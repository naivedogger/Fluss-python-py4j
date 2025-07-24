package org.example;

import com.alibaba.fluss.client.table.Table;

/**
 * Data reader factory - based on Fluss's data reading design
 * Only responsible for creating readers, no complex business logic
 */
public class FlussDataReaderFactory {
    
    /**
     * 创建扫描读取器
     * @param table 表实例
     * @param scanType 扫描类型 ("snapshot", "log", "stream")
     * @return 数据读取器
     */
    public FlussDataReader createScanReader(Table table, String scanType) {
        return new FlussDataReader(table, scanType);
    }
    
    /**
     * 创建查找读取器
     * @param table 表实例
     * @return 数据读取器
     */
    public FlussDataReader createLookupReader(Table table) {
        return new FlussDataReader(table, "lookup");
    }
}

package org.example;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;

/**
 * Data writer factory - based on Fluss's data writing design
 * Only responsible for creating writers, no complex business logic
 */
public class FlussDataWriterFactory {
    
    /**
     * 创建 Upsert 写入器
     * @param table 表实例
     * @return UpsertWriter
     */
    public UpsertWriter createUpsertWriter(Table table) {
        try {
            return table.newUpsert().createWriter();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create upsert writer: " + e.getMessage(), e);
        }
    }
    
    /**
     * 创建 Append 写入器  
     * @param table 表实例
     * @return AppendWriter
     */
    public AppendWriter createAppendWriter(Table table) {
        try {
            return table.newAppend().createWriter();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create append writer: " + e.getMessage(), e);
        }
    }
}

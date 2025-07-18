package org.example;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * FlussClientBridge 是一个用于Python客户端的Java桥接器
 * 通过Py4J提供Fluss客户端的核心功能
 *
 * 实现了以下主要功能：
 * 1. 连接管理
 * 2. 数据库和表管理
 * 3. 数据写入 (UpsertWriter, AppendWriter)
 * 4. 表模式管理
 * 5. 行数据操作
 * 6. 基础查询和管理操作
 */
public class FlussClientBridge {

    private Connection connection;
    private Admin admin;
    private final Map<String, Table> tableCache;
    private final FlussDataWriter dataWriter;

    public FlussClientBridge() {
        this.tableCache = new HashMap<>();
        this.dataWriter = new FlussDataWriter();
    }

    // ==================== 连接管理 ====================

    /**
     * 连接到Fluss集群
     * @param bootstrapServers 引导服务器地址，例如 "localhost:9123"
     * @return 连接是否成功
     */
    public boolean connect(String bootstrapServers) {
        try {
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.BOOTSTRAP_SERVERS, Arrays.asList(bootstrapServers));

            this.connection = ConnectionFactory.createConnection(conf);
            this.admin = connection.getAdmin();

            return true;
        } catch (Exception e) {
            System.err.println("连接失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 使用自定义配置连接到Fluss集群
     * @param configMap 配置映射
     * @return 连接是否成功
     */
    public boolean connectWithConfig(Map<String, String> configMap) {
        try {
            Configuration conf = new Configuration();

            // 设置基本配置
            for (Map.Entry<String, String> entry : configMap.entrySet()) {
                conf.setString(entry.getKey(), entry.getValue());
            }

            // 确保bootstrap servers配置正确
            if (configMap.containsKey("bootstrap.servers")) {
                conf.set(ConfigOptions.BOOTSTRAP_SERVERS,
                    Arrays.asList(configMap.get("bootstrap.servers").split(",")));
            }

            this.connection = ConnectionFactory.createConnection(conf);
            this.admin = connection.getAdmin();

            return true;
        } catch (Exception e) {
            System.err.println("连接失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // ==================== 数据库和表管理 ====================

    /**
     * 创建数据库
     * @param database 数据库名称
     * @param comment 数据库注释
     * @return 创建是否成功
     */
    public boolean createDatabase(String database, String comment) {
        try {
            DatabaseDescriptor.Builder builder = DatabaseDescriptor.builder();
            if (comment != null && !comment.trim().isEmpty()) {
                builder.comment(comment);
            }
            DatabaseDescriptor descriptor = builder.build();
            admin.createDatabase(database, descriptor, false).get();
            return true;
        } catch (Exception e) {
            System.err.println("创建数据库失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 创建数据库 (简化版本)
     * @param database 数据库名称
     * @return 创建是否成功
     */
    public boolean createDatabase(String database) {
        return createDatabase(database, "Created by Python client");
    }

    /**
     * 删除数据库
     * @param database 数据库名称
     * @return 删除是否成功
     */
    public boolean dropDatabase(String database) {
        try {
            admin.dropDatabase(database, false, false).get();
            return true;
        } catch (Exception e) {
            System.err.println("删除数据库失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 检查数据库是否存在
     * @param database 数据库名称
     * @return 数据库是否存在
     */
    public boolean databaseExists(String database) {
        try {
            return admin.databaseExists(database).get();
        } catch (Exception e) {
            System.err.println("检查数据库存在性失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 列出所有数据库
     * @return 数据库名称列表
     */
    public List<String> listDatabases() {
        try {
            CompletableFuture<List<String>> future = admin.listDatabases();
            return future.get();
        } catch (Exception e) {
            System.err.println("列出数据库失败: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * 列出指定数据库中的所有表
     * @param database 数据库名称
     * @return 表名称列表
     */
    public List<String> listTables(String database) {
        try {
            CompletableFuture<List<String>> future = admin.listTables(database);
            return future.get();
        } catch (Exception e) {
            System.err.println("列出表失败: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * 检查表是否存在
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 表是否存在
     */
    public boolean tableExists(String database, String tableName) {
        try {
            TablePath tablePath = TablePath.of(database, tableName);
            return admin.tableExists(tablePath).get();
        } catch (Exception e) {
            System.err.println("检查表存在性失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 获取表实例
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 表实例，如果获取失败返回null
     */
    public Table getTable(String database, String tableName) {
        try {
            String tableKey = database + "." + tableName;
            if (tableCache.containsKey(tableKey)) {
                return tableCache.get(tableKey);
            }

            TablePath tablePath = TablePath.of(database, tableName);
            Table table = connection.getTable(tablePath);
            tableCache.put(tableKey, table);

            return table;
        } catch (Exception e) {
            System.err.println("获取表失败: " + e.getMessage());
            return null;
        }
    }

    // ==================== 数据写入 ====================

    /**
     * 创建UpsertWriter
     * @param database 数据库名称
     * @param tableName 表名称
     * @return UpsertWriter实例
     */
    public UpsertWriter createUpsertWriter(String database, String tableName) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                return null;
            }

            return dataWriter.getOrCreateUpsertWriter(table, database, tableName);
        } catch (Exception e) {
            System.err.println("创建UpsertWriter失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 创建AppendWriter
     * @param database 数据库名称
     * @param tableName 表名称
     * @return AppendWriter实例
     */
    public AppendWriter createAppendWriter(String database, String tableName) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                return null;
            }

            return dataWriter.getOrCreateAppendWriter(table, database, tableName);
        } catch (Exception e) {
            System.err.println("创建AppendWriter失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 向日志表中追加数据
     * @param database 数据库名称
     * @param tableName 表名称
     * @param records 记录列表，每个记录是一个Map
     * @return 追加是否成功
     */
    public boolean appendData(String database, String tableName, List<Map<String, Object>> records) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                System.err.println("无法获取表: " + database + "." + tableName);
                return false;
            }

            // 使用FlussDataWriter进行批量Append操作
            return dataWriter.batchAppend(table, database, tableName, records);
            
        } catch (Exception e) {
            System.err.println("追加数据失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 向主键表中插入/更新单条记录
     * @param database 数据库名称
     * @param tableName 表名称
     * @param record 记录Map
     * @return 插入/更新是否成功
     */
    public boolean upsertRecord(String database, String tableName, Map<String, Object> record) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                System.err.println("无法获取表: " + database + "." + tableName);
                return false;
            }

            // 使用FlussDataWriter进行Upsert操作
            return dataWriter.upsertSingle(table, database, tableName, record);
            
        } catch (Exception e) {
            System.err.println("Upsert记录失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 向日志表中追加单条记录
     * @param database 数据库名称
     * @param tableName 表名称
     * @param record 记录Map
     * @return 追加是否成功
     */
    public boolean appendRecord(String database, String tableName, Map<String, Object> record) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                System.err.println("无法获取表: " + database + "." + tableName);
                return false;
            }

            // 使用FlussDataWriter进行Append操作
            return dataWriter.appendSingle(table, database, tableName, record);
            
        } catch (Exception e) {
            System.err.println("Append记录失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 从主键表中删除记录
     * @param database 数据库名称
     * @param tableName 表名称
     * @param record 包含主键信息的记录Map
     * @return 删除是否成功
     */
    public boolean deleteRecord(String database, String tableName, Map<String, Object> record) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                System.err.println("无法获取表: " + database + "." + tableName);
                return false;
            }

            // 使用FlussDataWriter进行Delete操作
            return dataWriter.deleteRecord(table, database, tableName, record);
            
        } catch (Exception e) {
            System.err.println("删除记录失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 刷新所有Writer缓存
     */
    public void flushAllWriters() {
        try {
            dataWriter.flushAll();
        } catch (Exception e) {
            System.err.println("刷新Writer失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 获取Writer统计信息
     * @return Writer统计信息
     */
    public Map<String, Integer> getWriterStats() {
        try {
            return dataWriter.getWriterStats();
        } catch (Exception e) {
            System.err.println("获取Writer统计信息失败: " + e.getMessage());
            e.printStackTrace();
            return new HashMap<>();
        }
    }

    // ==================== 数据读取 ====================

    /**
     * 创建ScanReader
     * @param database 数据库名称
     * @param tableName 表名称
     * @return FlussDataReader实例
     */
    public FlussDataReader createScanReader(String database, String tableName) {
        return createScanReader(database, tableName, "snapshot");
    }

    /**
     * 创建ScanReader，指定扫描类型
     * @param database 数据库名称
     * @param tableName 表名称
     * @param scanType 扫描类型 ("snapshot" 或 "log")
     * @return FlussDataReader实例
     */
    public FlussDataReader createScanReader(String database, String tableName, String scanType) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                return null;
            }

            FlussDataReader reader = new FlussDataReader(table, scanType);
            if (reader.initialize()) {
                return reader;
            } else {
                return null;
            }
        } catch (Exception e) {
            System.err.println("创建ScanReader失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 创建ScanReader，指定扫描类型和限制
     * @param database 数据库名称
     * @param tableName 表名称
     * @param scanType 扫描类型 ("snapshot" 或 "log")
     * @param limit 限制读取的记录数
     * @return FlussDataReader实例
     */
    public FlussDataReader createScanReader(String database, String tableName, String scanType, int limit) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                return null;
            }

            FlussDataReader reader = new FlussDataReader(table, scanType);
            if (reader.initialize(limit)) {
                return reader;
            } else {
                return null;
            }
        } catch (Exception e) {
            System.err.println("创建ScanReader失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 创建LookupReader
     * @param database 数据库名称
     * @param tableName 表名称
     * @return FlussDataReader实例
     */
    public FlussDataReader createLookupReader(String database, String tableName) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                return null;
            }

            FlussDataReader reader = new FlussDataReader(table, "lookup");
            if (reader.initialize()) {
                return reader;
            } else {
                return null;
            }
        } catch (Exception e) {
            System.err.println("创建LookupReader失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 创建StreamReader
     * @param database 数据库名称
     * @param tableName 表名称
     * @return FlussDataReader实例
     */
    public FlussDataReader createStreamReader(String database, String tableName) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                return null;
            }

            FlussDataReader reader = new FlussDataReader(table, "stream");
            if (reader.initialize()) {
                return reader;
            } else {
                return null;
            }
        } catch (Exception e) {
            System.err.println("创建StreamReader失败: " + e.getMessage());
            return null;
        }
    }

    /**
     * 读取表数据
     * @param database 数据库名称
     * @param tableName 表名称
     * @param maxRecords 最大记录数
     * @return 记录列表
     */
    public List<Map<String, Object>> readTableData(String database, String tableName, int maxRecords) {
        try {
            FlussDataReader reader = createScanReader(database, tableName, "snapshot", maxRecords);
            if (reader != null) {
                List<Map<String, Object>> records = reader.readRecords(maxRecords);
                reader.close();
                return records;
            }
        } catch (Exception e) {
            System.err.println("读取表数据失败: " + e.getMessage());
        }
        return new ArrayList<>();
    }

    /**
     * 读取表数据（简化版本）
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 记录列表（最多10条）
     */
    public List<Map<String, Object>> readTableData(String database, String tableName) {
        return readTableData(database, tableName, 10);
    }

    /**
     * 获取表的Schema信息
     * @param database 数据库名称
     * @param tableName 表名称
     * @return Schema信息的Map
     */
    public Map<String, Object> getTableSchema(String database, String tableName) {
        try {
            Table table = getTable(database, tableName);
            if (table != null) {
                // 这里需要根据实际的Fluss API获取Schema
                // 当前返回占位符数据
                Map<String, Object> schema = new HashMap<>();
                schema.put("database", database);
                schema.put("table", tableName);
                schema.put("status", "Schema获取功能需要根据实际API实现");
                return schema;
            }
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "获取Schema失败: " + e.getMessage());
            return error;
        }
        return new HashMap<>();
    }

    /**
     * 获取表的字段信息
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 字段信息列表
     */
    public List<Map<String, Object>> getTableFields(String database, String tableName) {
        try {
            Table table = getTable(database, tableName);
            if (table != null) {
                // 这里需要根据实际的Fluss API获取字段信息
                // 当前返回占位符数据
                List<Map<String, Object>> fields = new ArrayList<>();
                
                // 示例字段
                Map<String, Object> field1 = new HashMap<>();
                field1.put("name", "id");
                field1.put("type", "BIGINT");
                field1.put("nullable", false);
                field1.put("index", 0);
                fields.add(field1);
                
                Map<String, Object> field2 = new HashMap<>();
                field2.put("name", "name");
                field2.put("type", "STRING");
                field2.put("nullable", true);
                field2.put("index", 1);
                fields.add(field2);
                
                return fields;
            }
        } catch (Exception e) {
            System.err.println("获取字段信息失败: " + e.getMessage());
        }
        return new ArrayList<>();
    }

    /**
     * 查询表的记录数（估算）
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 记录数估算值
     */
    public long getTableRecordCount(String database, String tableName) {
        try {
            Table table = getTable(database, tableName);
            if (table != null) {
                // 这里需要根据实际的Fluss API获取记录数
                // 当前返回占位符数据
                System.out.println("获取记录数功能需要根据实际API实现");
                return -1; // 表示不可用
            }
        } catch (Exception e) {
            System.err.println("获取记录数失败: " + e.getMessage());
        }
        return -1;
    }

    /**
     * 检查表是否为空
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 是否为空
     */
    public boolean isTableEmpty(String database, String tableName) {
        try {
            List<Map<String, Object>> records = readTableData(database, tableName, 1);
            return records.isEmpty();
        } catch (Exception e) {
            System.err.println("检查表是否为空失败: " + e.getMessage());
            return true;
        }
    }

    /**
     * 执行表的健康检查
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 健康检查结果
     */
    public Map<String, Object> checkTableHealth(String database, String tableName) {
        Map<String, Object> health = new HashMap<>();
        
        try {
            // 检查表是否存在
            boolean exists = tableExists(database, tableName);
            health.put("exists", exists);
            
            if (exists) {
                // 检查是否可以获取表实例
                Table table = getTable(database, tableName);
                health.put("accessible", table != null);
                
                if (table != null) {
                    // 检查是否可以创建读取器
                    FlussDataReader reader = createScanReader(database, tableName);
                    health.put("readable", reader != null);
                    
                    if (reader != null) {
                        // 尝试读取一条记录
                        List<Map<String, Object>> records = reader.readRecords(1);
                        health.put("has_data", !records.isEmpty());
                        health.put("sample_record_count", records.size());
                        reader.close();
                    }
                }
            }
            
            health.put("status", "healthy");
            health.put("check_time", System.currentTimeMillis());
            
        } catch (Exception e) {
            health.put("status", "error");
            health.put("error", "健康检查失败: " + e.getMessage());
        }
        
        return health;
    }
    // ==================== 行数据操作 ====================

    /**
     * 创建GenericRow
     * @param arity 行的列数
     * @return GenericRow实例
     */
    public GenericRow createGenericRow(int arity) {
        return new GenericRow(arity);
    }

    /**
     * 设置GenericRow的字段值
     * @param row GenericRow实例
     * @param pos 字段位置
     * @param value 字段值
     */
    public void setGenericRowField(GenericRow row, int pos, Object value) {
        row.setField(pos, value);
    }

    /**
     * 获取GenericRow的字段值
     * @param row GenericRow实例
     * @param pos 字段位置
     * @return 字段值
     */
    public Object getGenericRowField(GenericRow row, int pos) {
        return row.getField(pos);
    }

    // ==================== 连接状态和清理 ====================

    /**
     * 检查连接状态
     * @return 连接是否有效
     */
    public boolean isConnected() {
        return connection != null && admin != null;
    }

    /**
     * 关闭所有缓存的读写器
     */
    public void closeAllWriters() {
        try {
            // 使用DataWriter关闭所有writers
            dataWriter.closeAll();
        } catch (Exception e) {
            System.err.println("关闭读写器失败: " + e.getMessage());
        }
    }

    /**
     * 关闭连接
     */
    public void close() {
        try {
            // 首先关闭所有读写器
            closeAllWriters();

            // 清空表缓存
            tableCache.clear();

            // 关闭连接
            if (connection != null) {
                connection.close();
                connection = null;
            }

            admin = null;

        } catch (Exception e) {
            System.err.println("关闭连接失败: " + e.getMessage());
        }
    }

    // ==================== 表管理 ====================

    /**
     * 创建表
     * @param database 数据库名称
     * @param tableName 表名称
     * @param fieldNames 字段名称列表
     * @param fieldTypes 字段类型列表
     * @param primaryKeys 主键列表
     * @return 创建是否成功
     */
    public boolean createTable(String database, String tableName, List<String> fieldNames, List<String> fieldTypes, List<String> primaryKeys) {
        try {
            if (fieldNames.size() != fieldTypes.size()) {
                System.err.println("字段名称和类型数量不匹配");
                return false;
            }

            // 构建Schema
            Schema.Builder schemaBuilder = Schema.newBuilder();
            
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                String fieldType = fieldTypes.get(i);
                
                // 根据类型字符串创建DataType
                DataType dataType = parseDataType(fieldType);
                schemaBuilder.column(fieldName, dataType);
            }
            
            // 设置主键
            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                schemaBuilder.primaryKey(primaryKeys);
            }
            
            Schema schema = schemaBuilder.build();
            
            // 创建表描述符
            TableDescriptor tableDescriptor = TableDescriptor.builder()
                .schema(schema)
                .build();
            
            // 创建表
            TablePath tablePath = TablePath.of(database, tableName);
            admin.createTable(tablePath, tableDescriptor, false).get();
            
            System.out.println("表创建成功: " + tablePath);
            return true;
            
        } catch (Exception e) {
            System.err.println("创建表失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 创建简单表（只有id和name字段）
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 创建是否成功
     */
    public boolean createSimpleTable(String database, String tableName) {
        List<String> fieldNames = Arrays.asList("id", "name", "age", "created_time");
        List<String> fieldTypes = Arrays.asList("BIGINT", "STRING", "INTEGER", "TIMESTAMP(3)");
        List<String> primaryKeys = Arrays.asList("id");
        
        return createTable(database, tableName, fieldNames, fieldTypes, primaryKeys);
    }

    /**
     * 解析数据类型字符串
     * @param typeString 类型字符串
     * @return DataType
     */
    private DataType parseDataType(String typeString) {
        switch (typeString.toUpperCase()) {
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INTEGER":
            case "INT":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "STRING":
                return DataTypes.STRING();
            case "BYTES":
                return DataTypes.BYTES();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "TIMESTAMP(3)":
                return DataTypes.TIMESTAMP(3);
            default:
                System.err.println("未知的数据类型: " + typeString + "，使用STRING类型");
                return DataTypes.STRING();
        }
    }

    /**
     * 删除表
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 删除是否成功
     */
    public boolean dropTable(String database, String tableName) {
        try {
            TablePath tablePath = TablePath.of(database, tableName);
            admin.dropTable(tablePath, false).get();
            
            // 从缓存中移除
            String tableKey = database + "." + tableName;
            tableCache.remove(tableKey);
            
            System.out.println("表删除成功: " + tablePath);
            return true;
            
        } catch (Exception e) {
            System.err.println("删除表失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 向表中插入数据
     * @param database 数据库名称
     * @param tableName 表名称
     * @param records 记录列表，每个记录是一个Map
     * @return 插入是否成功
     */
    public boolean insertData(String database, String tableName, List<Map<String, Object>> records) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                System.err.println("无法获取表: " + database + "." + tableName);
                return false;
            }

            // 使用FlussDataWriter进行批量Upsert操作
            return dataWriter.batchUpsert(table, database, tableName, records);
            
        } catch (Exception e) {
            System.err.println("插入数据失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // ==================== 访问器方法 ====================

    /**
     * 获取Admin客户端
     * @return Admin实例
     */
    public Admin getAdmin() {
        return admin;
    }

    /**
     * 获取Connection实例
     * @return Connection实例
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * 获取表的基本信息
     * @param database 数据库名称
     * @param tableName 表名称
     * @return 表信息的字符串表示
     */
    public String getTableInfo(String database, String tableName) {
        try {
            Table table = getTable(database, tableName);
            if (table == null) {
                return "表不存在";
            }

            StringBuilder info = new StringBuilder();
            info.append("表: ").append(database).append(".").append(tableName).append("\n");
            info.append("表实例获取成功");

            return info.toString();
        } catch (Exception e) {
            System.err.println("获取表信息失败: " + e.getMessage());
            return "获取表信息失败: " + e.getMessage();
        }
    }

    /**
     * 获取连接配置信息
     * @return 配置信息的字符串表示
     */
    public String getConnectionInfo() {
        if (connection == null) {
            return "未连接";
        }

        try {
            StringBuilder info = new StringBuilder();
            info.append("连接状态: 已连接\n");
            info.append("数据库数量: ").append(listDatabases().size()).append("\n");

            List<String> databases = listDatabases();
            for (String db : databases) {
                info.append("数据库 ").append(db).append(" 表数量: ").append(listTables(db).size()).append("\n");
            }

            return info.toString();
        } catch (Exception e) {
            return "获取连接信息失败: " + e.getMessage();
        }
    }

    /**
     * 获取数据库信息
     * @param database 数据库名称
     * @return 数据库信息的字符串表示
     */
    public String getDatabaseInfo(String database) {
        try {
            if (!databaseExists(database)) {
                return "数据库不存在";
            }

            StringBuilder info = new StringBuilder();
            info.append("数据库: ").append(database).append("\n");

            List<String> tables = listTables(database);
            info.append("表数量: ").append(tables.size()).append("\n");

            if (!tables.isEmpty()) {
                info.append("表列表:\n");
                for (String table : tables) {
                    info.append("  - ").append(table).append("\n");
                }
            }

            return info.toString();
        } catch (Exception e) {
            System.err.println("获取数据库信息失败: " + e.getMessage());
            return "获取数据库信息失败: " + e.getMessage();
        }
    }
}

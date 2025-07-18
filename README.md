# Fluss Python Py4J 桥接器

这个项目通过Py4J创建了一个Java-Python桥接器，让Python客户端能够使用Fluss数据库的功能。

## 项目结构

```
fluss-python-py4j/
├── pom.xml                          # Maven配置文件
├── src/main/java/org/example/
│   ├── FlussClientBridge.java       # Fluss客户端桥接器
│   ├── FlussDataReader.java         # Fluss数据读取器
│   ├── FlussDataWriter.java         # Fluss数据写入器
│   ├── Py4JGatewayServer.java      # Py4J网关服务器
│   └── SimplePy4JGatewayServer.java # 简化的Py4J网关服务器
├── test_data_writer.py              # Python测试脚本
└── README.md                        # 使用说明
```

## 依赖项

- **Fluss Client**: 用于连接Fluss数据库
- **Py4J**: Java-Python桥接库
- **JDK 8+**: Java运行环境
- **Python 3.x**: Python运行环境

## 安装和设置

### 1. 构建Java项目

```bash
# 编译项目
mvn clean compile

# 或者打包成JAR文件
mvn clean package
```

### 2. 安装Python依赖

```bash
pip install py4j
```

## 使用方法

### 启动Java网关服务器

在一个终端中运行：

```bash
# 使用默认端口25333
java -cp target/classes org.example.Py4JGatewayServer

# 或指定端口
java -cp target/classes org.example.Py4JGatewayServer 25334
```

### 运行Python客户端

在另一个终端中运行：

```bash
# 使用默认Fluss服务器地址运行测试脚本
python test_data_writer.py

# 或指定服务器地址
python test_data_writer.py localhost:9123
```

## 功能特性

### FlussClientBridge 提供的功能：

1. **连接管理**
   - `connect(bootstrapServers)`: 连接到Fluss集群
   - `isConnected()`: 检查连接状态
   - `close()`: 关闭连接

2. **数据库和表操作**
   - `listDatabases()`: 列出所有数据库
   - `listTables(database)`: 列出指定数据库中的表
   - `getTable(database, tableName)`: 获取表实例

3. **读写操作**
   - `createUpsertWriter(database, tableName)`: 创建更新写入器
   - `createScanReader(database, tableName)`: 创建扫描读取器

### Python客户端功能：

- 自动连接到Py4J网关
- 封装所有Java桥接器功能
- 提供友好的Python接口
- 错误处理和连接管理

## 使用示例

### Java端示例

```java
FlussClientBridge bridge = new FlussClientBridge();
bridge.connect("localhost:9123");

List<String> databases = bridge.listDatabases();
for (String db : databases) {
    System.out.println("Database: " + db);
    List<String> tables = bridge.listTables(db);
    for (String table : tables) {
        System.out.println("  Table: " + table);
    }
}

bridge.close();
```

### Python端示例

```python
from py4j.java_gateway import JavaGateway

# 连接到网关
gateway = JavaGateway()
bridge = gateway.entry_point

# 连接到Fluss
bridge.connect("localhost:9123")

# 列出数据库
databases = bridge.listDatabases()
for db in databases:
    print(f"Database: {db}")
    tables = bridge.listTables(db)
    for table in tables:
        print(f"  Table: {table}")

# 关闭连接
bridge.close()
```

## 故障排除

### 常见问题

1. **连接失败**
   - 确保Fluss服务器正在运行
   - 检查服务器地址和端口是否正确

2. **Py4J网关连接失败**
   - 确保Java网关服务器正在运行
   - 检查端口是否被占用

3. **依赖项错误**
   - 运行 `mvn clean compile` 重新编译
   - 确保所有依赖项都已正确安装

### 调试模式

启动网关服务器时可以查看详细日志：

```bash
java -cp target/classes org.example.Py4JGatewayServer
```

## 扩展功能

可以通过以下方式扩展桥接器：

1. 在`FlussClientBridge`中添加更多Fluss客户端功能
2. 在Python客户端中添加更高级的封装
3. 添加异步操作支持
4. 实现连接池管理

## 注意事项

- 确保Java网关服务器在Python客户端启动之前运行
- 网关服务器默认监听25333端口
- 建议在生产环境中使用连接池和错误重试机制

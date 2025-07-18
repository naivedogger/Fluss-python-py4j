#!/usr/bin/env python3
"""
测试FlussDataWriter功能的Python脚本
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__)))

from py4j.java_gateway import JavaGateway, GatewayParameters
import time

def test_data_writer():
    """测试FlussDataWriter的各种写入功能"""
    
    # 连接到Java网关
    gateway = JavaGateway(gateway_parameters=GatewayParameters(port=25333))
    
    # 获取FlussClientBridge实例
    bridge = gateway.entry_point
    
    # 连接到Fluss集群
    print("连接到Fluss集群...")
    success = bridge.connect("localhost:9123")
    if not success:
        print("连接失败")
        return False
    
    print("连接成功")
    
    # 测试参数
    database = "test_db"
    table_name = "test_write_table"
    
    try:
        # 创建数据库
        print(f"创建数据库: {database}")
        bridge.createDatabase(database)
        
        # 创建表
        print(f"创建表: {table_name}")
        success = bridge.createSimpleTable(database, table_name)
        if not success:
            print("创建表失败")
            return False
        
        # 测试批量Upsert
        print("\n=== 测试批量Upsert ===")
        records = [
            {"id": 1, "name": "Alice", "age": 25, "created_time": "2024-01-01 10:00:00"},
            {"id": 2, "name": "Bob", "age": 30, "created_time": "2024-01-01 11:00:00"},
            {"id": 3, "name": "Charlie", "age": 35, "created_time": "2024-01-01 12:00:00"}
        ]
        
        # 转换为Java List
        java_records = gateway.jvm.java.util.ArrayList()
        for record in records:
            java_map = gateway.jvm.java.util.HashMap()
            for key, value in record.items():
                java_map[key] = value
            java_records.append(java_map)
        
        success = bridge.insertData(database, table_name, java_records)
        if success:
            print("批量Upsert成功")
        else:
            print("批量Upsert失败")
            return False
        
        # 测试单条Upsert
        print("\n=== 测试单条Upsert ===")
        single_record = {"id": 4, "name": "Diana", "age": 28, "created_time": "2024-01-01 13:00:00"}
        java_single_map = gateway.jvm.java.util.HashMap()
        for key, value in single_record.items():
            java_single_map[key] = value
        
        success = bridge.upsertRecord(database, table_name, java_single_map)
        if success:
            print("单条Upsert成功")
        else:
            print("单条Upsert失败")
        
        # 测试更新存在的记录
        print("\n=== 测试更新存在的记录 ===")
        update_record = {"id": 1, "name": "Alice Updated", "age": 26, "created_time": "2024-01-01 14:00:00"}
        java_update_map = gateway.jvm.java.util.HashMap()
        for key, value in update_record.items():
            java_update_map[key] = value
        
        success = bridge.upsertRecord(database, table_name, java_update_map)
        if success:
            print("更新记录成功")
        else:
            print("更新记录失败")
        
        # 读取数据验证
        print("\n=== 读取数据验证 ===")
        time.sleep(1)  # 等待数据写入完成
        
        data = bridge.readTableData(database, table_name, 10)
        if data:
            print(f"读取到 {len(data)} 条记录:")
            for i, record in enumerate(data):
                print(f"  记录 {i+1}: {dict(record)}")
        else:
            print("没有读取到数据")
        
        # 获取Writer统计信息
        print("\n=== Writer统计信息 ===")
        stats = bridge.getWriterStats()
        print(f"Writer统计: {dict(stats)}")
        
        # 刷新所有Writers
        print("\n=== 刷新所有Writers ===")
        bridge.flushAllWriters()
        print("Writers刷新完成")
        
        # 测试删除记录
        print("\n=== 测试删除记录 ===")
        delete_record = {"id": 3, "name": "Charlie", "age": 35, "created_time": "2024-01-01 12:00:00"}
        java_delete_map = gateway.jvm.java.util.HashMap()
        for key, value in delete_record.items():
            java_delete_map[key] = value
        
        success = bridge.deleteRecord(database, table_name, java_delete_map)
        if success:
            print("删除记录成功")
        else:
            print("删除记录失败")
        
        # 再次读取数据验证删除
        print("\n=== 验证删除结果 ===")
        time.sleep(1)
        data = bridge.readTableData(database, table_name, 10)
        if data:
            print(f"删除后读取到 {len(data)} 条记录:")
            for i, record in enumerate(data):
                print(f"  记录 {i+1}: {dict(record)}")
        else:
            print("没有读取到数据")
        
        return True
        
    except Exception as e:
        print(f"测试过程中发生错误: {str(e)}")
        return False
    
    finally:
        # 清理资源
        print("\n=== 清理资源 ===")
        try:
            bridge.dropTable(database, table_name)
            print(f"删除表: {table_name}")
        except:
            pass
        
        try:
            bridge.dropDatabase(database)
            print(f"删除数据库: {database}")
        except:
            pass
        
        try:
            bridge.close()
            print("关闭连接")
        except:
            pass

if __name__ == "__main__":
    print("FlussDataWriter 功能测试")
    print("=" * 50)
    
    success = test_data_writer()
    
    if success:
        print("\n✅ 所有测试通过!")
    else:
        print("\n❌ 测试失败!")
        sys.exit(1)

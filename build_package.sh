#!/bin/bash
# 构建和安装 PyFluss 包的脚本

set -e  # 遇到错误立即退出

echo "🔧 构建 PyFluss Python 包..."

# 1. 清理之前的构建文件
echo "1. 清理之前的构建文件..."
rm -rf build/
rm -rf dist/
rm -rf *.egg-info/
rm -rf pyfluss.egg-info/

# 2. 确保Java JAR文件存在
echo "2. 检查Java JAR文件..."
if [ ! -f "target/fluss-python-py4j-1.0-SNAPSHOT.jar" ]; then
    echo "   编译Java项目..."
    mvn clean compile package -DskipTests
fi

if [ ! -f "pyfluss/jars/fluss-python-py4j-1.0-SNAPSHOT.jar" ]; then
    echo "   复制JAR文件到包目录..."
    cp target/fluss-python-py4j-1.0-SNAPSHOT.jar pyfluss/jars/
fi

echo "   ✓ JAR文件准备完成"

# 3. 构建源码分发包和wheel包
echo "3. 构建Python包..."
python -m pip install --upgrade pip setuptools wheel build
python -m build

echo "   ✓ 包构建完成"

# 4. 列出构建的文件
echo "4. 构建的文件:"
ls -la dist/

# 5. 询问是否安装
echo ""
read -p "是否要安装到当前环境？(y/n): " install_choice

if [[ $install_choice == "y" || $install_choice == "Y" ]]; then
    echo "5. 安装PyFluss包..."
    
    # 卸载旧版本（如果存在）
    pip uninstall pyfluss -y 2>/dev/null || true
    
    # 安装新版本
    pip install dist/pyfluss-*.whl
    
    echo "   ✓ PyFluss安装完成"
    
    # 6. 验证安装
    echo "6. 验证安装..."
    python -c "import pyfluss; print(f'PyFluss version: {pyfluss.__version__}')"
    python -c "from pyfluss import Catalog; print('✓ 导入测试成功')"
    
    echo ""
    echo "🎉 PyFluss包构建和安装成功！"
    echo ""
    echo "现在你可以使用以下方式导入："
    echo "  import pyfluss"
    echo "  from pyfluss import Catalog, Table"
    echo "  from pyfluss.py4j import get_gateway"
    
else
    echo "5. 跳过安装"
    echo ""
    echo "✓ PyFluss包构建完成！"
    echo ""
    echo "要安装包，请运行："
    echo "  pip install dist/pyfluss-*.whl"
fi

echo ""
echo "📦 包文件位置: $(pwd)/dist/"
echo "🔧 如需发布到PyPI，请运行: twine upload dist/*"

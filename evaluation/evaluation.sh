#!/usr/bin/env bash

path=$(pwd)
echo ${path}

python_path=${PYTHON_PATH:-"python3"}
pip_path=${PIP_PATH:-"pip3"}

requirementPath=$path/requirements.txt
${pip_path} install -r ${requirementPath}
echo "安装Python模块成功"

# 构建表和模型
${python_path} $path/build_tables.py
${python_path} $path/build_models.py

# 参数检查
dataset=${1:-"dusql"}  # 默认为dusql，可以通过参数指定tvshow

# 运行评估
echo "开始评估数据集: ${dataset}"
${python_path} $path/evaluation.py --dataset ${dataset}

/* Copyright (c) 2023 Renmin University of China
 * RMDB is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

// 抽象执行器基类
// 定义了查询执行器的统一接口，具体执行器需要继承并实现这些接口
class AbstractExecutor {
   public:
    Rid _abstract_rid;        // 记录标识符，用于定位当前处理的记录

    Context *context_;        // 执行上下文，包含事务、缓冲区管理等运行时信息

    virtual ~AbstractExecutor() = default;  // 虚析构函数，确保派生类正确释放资源

    // 获取元组长度（字节数）
    virtual size_t tupleLen() const { return 0; };

    // 获取列元数据信息
    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    // 获取执行器类型名称
    virtual std::string getType() { return "AbstractExecutor"; };

    // 开始处理元组（初始化迭代器）
    virtual void beginTuple(){};

    // 移动到下一个元组
    virtual void nextTuple(){};

    // 判断是否已处理完所有元组
    virtual bool is_end() const { return true; };

    // 获取当前记录的RID（记录标识符）
    virtual Rid &rid() = 0;

    // 获取下一个记录（核心方法，由具体执行器实现）
    virtual std::unique_ptr<RmRecord> Next() = 0;

    // 根据表名列名获取列元数据信息
    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    // 在列元数据集合中查找指定表列
    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        // 使用Lambda表达式查找匹配的列
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        // 如果未找到，抛出列不存在异常
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }
};
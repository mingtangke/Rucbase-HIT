/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    file_handle_ = file_handle;  
    rid_ = {RM_FIRST_RECORD_PAGE, -1};
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    while(this->rid_.page_no < file_handle_ -> file_hdr_.num_pages){
        RmPageHandle page_handle = file_handle_->fetch_page_handle(this->rid_.page_no);
        this->rid_.slot_no = Bitmap::next_bit(true, page_handle.bitmap,
                                              file_handle_->file_hdr_.num_records_per_page,
                                              this->rid_.slot_no);
        if(this->rid_.slot_no >= this->file_handle_->file_hdr_.num_records_per_page){  //本页没有
            if ((this->rid_.page_no + 1) == file_handle_ -> file_hdr_.num_pages){ // 遍历后续所有页，未找到
                this->rid_ = Rid{RM_NO_PAGE, -1};
                break;
            }
            else{
                this->rid_ = Rid{this->rid_.page_no+1, -1};  //获取下一页，下一个循环继续找下一页
            }
        }
        else{
            break;
        }
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值

    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}
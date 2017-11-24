//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-12-23</createDate>
//<description>获取单据及条码服务 接口</description>
//---------------------------------------------------------------- 

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business{

    /// <summary>
    /// 获取单据及条码服务 接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取单据及条码服务 接口")]
    public interface IGetDocAndBarcodeService{
        /// <summary>
        /// 获取单据及条码服务
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="scan_type">扫描类型</param>
        /// <param name="analysis_symbol">解析符号</param>
        /// <param name="status">执行动作</param>
        /// <param name="barcode_no">条形码编号</param>
        /// <param name="warehouse_no">库位编号</param>
        /// <param name="storage_spaces_no">储位编号</param>
        /// <param name="lot_no">批号</param>
        /// <param name="inventory_management_features">库存管理特征</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        Hashtable GetDocAndBarcode(string program_job_no, string scan_type, string analysis_symbol, string status, 
            string barcode_no, string warehouse_no, string storage_spaces_no, string lot_no, 
            string inventory_management_features, string site_no);
    }
}
//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-19</createDate>
//<description>获取条码接口</description>
//---------------------------------------------------------------- 
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取条码接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取条码接口")]
    public interface IGetBarcodeNewService {
        /// <summary>
        /// 获取条码接口
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">执行动作</param>
        /// <param name="barcode_no">条码编号</param>
        /// <param name="warehouse_no">仓库编号</param>
        /// <param name="storage_spaces_no">库位编号</param>
        /// <param name="lot_no">批号</param>
        /// <param name="inventory_management_features">库存管理特征</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        Hashtable GetBarcodeNew(string program_job_no, string status, string barcode_no, string warehouse_no, 
            string storage_spaces_no, string lot_no, string inventory_management_features, string site_no);
    }
}

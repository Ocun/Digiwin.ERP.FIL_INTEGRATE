//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-13</createDate>
//<description>获取采购未交明细接口</description>
//---------------------------------------------------------------- 
//20170919 modi by liwei1 for B001-170918003        

using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取采购未交明细接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取采购未交明细接口")]
    public interface IPurchaseOrderUaqtyService {
        /// <summary>
        /// 根据传入的供应商查询出所有的采购未交明细
        /// </summary>
        /// <param name="supplier_no">供应商编号(必填)</param>
        /// <param name="date_s">订单日期起</param>
        /// <param name="date_e">订单日期止</param>
        /// <param name="due_date_s">预交日期起</param>
        /// <param name="due_date_e">预交日期止</param>
        /// <param name="item_no">料件编号</param>
        /// <param name="item_name">品名</param>
        /// <param name="item_spec">规格</param>
        /// <param name="qc_type">检验否</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <param name="purchase_no">采购单号</param>
        /// <returns></returns>
        Hashtable PurchaseOrderUaqty(string supplier_no, string date_s, string date_e, string due_date_s, string due_date_e,
            string item_no, string item_name, string item_spec, string qc_type, string enterprise_no, string site_no
            , string purchase_no//20170919 add by liwei1 for B001-170918003             
            );
    }
}

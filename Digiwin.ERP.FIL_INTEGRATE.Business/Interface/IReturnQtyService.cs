//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-13</createDate>
//<description>退换货清单查询接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 退换货清单查询接口
    /// </summary>
    [TypeKeyOnly]
    [Description("退换货清单查询接口")]
    public interface IReturnQtyService {
        /// <summary>
        /// 根据传入的供应商查询出退换货清单
        /// </summary>
        /// <param name="supplier_no">供应商编号(必填)</param>
        /// <param name="item_no">料件编号</param>
        /// <param name="item_name">品名</param>
        /// <param name="item_spec">规格</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <returns></returns>
        Hashtable ReturnQty(string supplier_no, string item_no, string item_name, string item_spec, string enterprise_no, string site_no);
    }
}

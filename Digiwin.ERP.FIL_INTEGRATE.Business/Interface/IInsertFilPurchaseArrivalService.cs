//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-03-26</createDate>
//<docNo>P001-170316001<docNo>
//<description>依送货单生成到货单接口</description>
//---------------------------------------------------------------- 

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 依送货单生成到货单接口
    /// </summary>
    [TypeKeyOnly]
    [Description("依送货单生成到货单接口")]
    public interface IInsertFilPurchaseArrivalService {
        /// <summary>
        /// 产生到货单信息
        /// </summary>
        /// <param name="receipt_detail">receipt_detail集合包括：送货单号（delivery_no）、品号（item_no）、特征码（item_feature_no）、数量（qty）</param>
        /// <returns>单据编号</returns>
        Hashtable InsertFilPurchaseArrival(DependencyObjectCollection receipt_detail);
    }
}

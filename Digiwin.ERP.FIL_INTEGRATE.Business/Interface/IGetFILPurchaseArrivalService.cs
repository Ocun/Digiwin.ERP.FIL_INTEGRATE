//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-03-24</createDate>
//<description>获取送货单明细接口</description>
//---------------------------------------------------------------- 

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取送货单明细接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取送货单明细接口")]
    public interface IGetFILPurchaseArrivalService {
        /// <summary>
        /// 获取送货单明细
        /// </summary>
        /// <param name="delivery_no">送货单号</param>
        /// <returns></returns>
        Hashtable GetFilPurchaseArrival(string delivery_no);
    }
}

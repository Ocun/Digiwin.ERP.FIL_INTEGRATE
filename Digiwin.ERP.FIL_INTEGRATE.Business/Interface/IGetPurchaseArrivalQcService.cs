//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-04-18</createDate>
//<description>获取到货单质检接口</description>
//---------------------------------------------------------------- 

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///获取到货单服务 接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取到货单质检接口")]
    public interface IGetPurchaseArrivalQcService {

        /// <summary>
        /// 根据传入的条码，获取相应的到货单信息
        /// </summary>
        /// <param name="barcode_no">扫描单号</param>
        /// <returns></returns>
        Hashtable GetPurchaseArrivalQc(string barcode_no);
    }
}
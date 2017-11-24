//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-14</createDate>
//<description>条码信息获取接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 条码信息获取接口
    /// </summary>
    [TypeKeyOnly]
    [Description("条码信息获取接口")]
    public interface IGetItemBcRecodeService {
        /// <summary>
        /// 根据传入的送货单单号信息获取条码信息档
        /// </summary>
        /// <param name="delivery_no">送货单号</param>
        /// <returns></returns>
        Hashtable GetItemBcRecode(string delivery_no);
    }
}

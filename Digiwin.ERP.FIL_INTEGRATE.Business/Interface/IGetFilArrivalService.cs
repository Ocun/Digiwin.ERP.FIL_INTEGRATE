//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-15</createDate>
//<description>送货单号获取接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 送货单号获取接口
    /// </summary>
    [TypeKeyOnly]
    [Description("送货单号获取接口")]
    public interface IGetFilArrivalService {
        /// <summary>
        /// 根据传入的供应商等信息获取送货单单号
        /// </summary>
        /// <param name="supplier_no">供应商编号</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <returns></returns>
        Hashtable GetFilArrival(string supplier_no, string enterprise_no, string site_no);
    }
}

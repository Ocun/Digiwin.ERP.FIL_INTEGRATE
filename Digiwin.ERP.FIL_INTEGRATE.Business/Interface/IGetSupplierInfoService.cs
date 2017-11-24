//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-15</createDate>
//<description>供应商平台信息获取接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 供应商平台信息获取接口
    /// </summary>
    [TypeKeyOnly]
    [Description("供应商平台信息获取接口")]
    public interface IGetSupplierInfoService {
        /// <summary>
        /// 根据传入的供应商编号获取供应商其他相关信息
        /// </summary>
        /// <param name="supplier_no">供应商编号</param>
        /// <returns></returns>
        Hashtable GetSupplierInfo(string supplier_no);
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-12</createDate>
//<description>供应商检查接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 供应商检查接口
    /// </summary>
    [TypeKeyOnly]
    [Description("供应商检查接口")]
    public interface ISupplierService {
        /// <summary>
        /// 根据传入的供应商编号检查供应商是否存在，若存在返回供应商编号和名称
        /// </summary>
        /// <param name="supplier_no">供应商编号</param>
        /// <returns></returns>
        Hashtable Supplier(string supplier_no);
    }
}

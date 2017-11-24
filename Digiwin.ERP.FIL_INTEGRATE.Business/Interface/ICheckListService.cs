//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-12</createDate>
//<description>对账清单接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 对账清单接口
    /// </summary>
    [TypeKeyOnly]
    [Description("对账清单接口")]
    public interface ICheckListService {
        /// <summary>
        /// 根据传入的供应商查询出所有的对账清单
        /// </summary>
        /// <param name="supplier_no">供应商编号(必填)</param>
        /// <param name="date_s">对账开始日期</param>
        /// <param name="date_e">对账截止日期</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <returns></returns>
        Hashtable CheckList(string supplier_no, string date_s, string date_e, string enterprise_no, string site_no);
    }
}

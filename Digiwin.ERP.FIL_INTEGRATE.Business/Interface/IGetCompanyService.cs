//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-04-28</createDate>
//<description>获取企业信息接口</description>
//---------------------------------------------------------------- 

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {

    /// <summary>
    /// 获取企业信息
    /// </summary>
    [TypeKeyOnly]
    [Description("获取企业信息接口")]
    public interface IGetCompanyService {

        /// <summary>
        /// 获取企业信息
        /// </summary>
        /// <returns></returns>
        Hashtable GetCompany();
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-17</createDate>
//<description>获取使用者信息接口</description>
//---------------------------------------------------------------- 
//20161222 add by liwei1 for P001-161215001
using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {

    /// <summary>
    /// 获取使用者信息接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取使用者信息接口")]
    public interface IGetUserService {

        /// <summary>
        /// 根据传入的账号，获取相应的用户信息
        /// </summary>
        /// <param name="account">账号</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        Hashtable GetUser(string account);//20161222 add by liwei1 for P001-161215001
        //Hashtable GetUser(string account, string site_no);//20161222 mark by liwei1 for P001-161215001
    }
}

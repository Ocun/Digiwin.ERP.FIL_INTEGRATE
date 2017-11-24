//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-17</createDate>
//<description>获取使用者库存信息接口</description>
//---------------------------------------------------------------- 

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {

    /// <summary>
    /// 获取使用者库存信息接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取使用者库存信息接口")]
    public interface IGetUserInventoryService {

        /// <summary>
        /// 获取相应的用户信息
        /// </summary>
        /// <param name="hashkey">使用者账号、密码</param>
        /// <param name="report_datetime">上传时间：暂时没有启用这个参数</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        Hashtable GetUserInventory(string hashkey, string report_datetime, string site_no);
    }
}

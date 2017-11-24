//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-15</createDate>
//<description>料件信息获取接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {

    [TypeKeyOnly]
    [Description("料件信息获取接口")]
    public interface IGetItemInfoService {
        /// <summary>
        /// 根据传入的品号，查询品号的其他相关信息
        /// </summary>
        /// <param name="item_no">料件编号</param>
        /// <returns></returns>
        Hashtable GetItemInfo(string item_no);
    }
}

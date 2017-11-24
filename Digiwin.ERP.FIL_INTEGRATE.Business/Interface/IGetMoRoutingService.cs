//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/14 13:49:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>获取工单工艺信息服务接口</description>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    public interface IGetMoRoutingService {
        /// <summary>
        /// 获取工单工艺信息服务
        /// </summary>
        /// <param name="barcode_no">二维码</param>
        /// <param name="analysis_symbol">解析符号</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="report_type">报工类别</param>
        /// <returns></returns>
        Hashtable GetMoRouting(string barcode_no, string analysis_symbol, string site_no, string report_type);
    }
}

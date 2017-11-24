//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>shenbao</author>
//<createDate>2017/08/01 13:49:37</createDate>
//<IssueNo>P001-170717001</IssueNo>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("获取未入库工单通知服务接口")]
    public interface IGetMOListService {
        /// <summary>
        /// 获取未入库工单通知服务
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetMOList(string programJobNo, string scanType, string status, string siteNo
            , DependencyObjectCollection condition
            );
    }
}

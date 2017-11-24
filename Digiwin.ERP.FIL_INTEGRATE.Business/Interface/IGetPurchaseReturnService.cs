//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>wangrm</author>
//<createDate>2017/02/10 13:49:37</createDate>
//<IssueNo>P001-170207001</IssueNo>
//<description>获取采购退货单</description>

using Digiwin.Common;
using Digiwin.Common.Torridity;
using System.ComponentModel;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("获取采购退货单服务接口")]
    public interface IGetPurchaseReturnService {
        /// <summary>
        /// 查询采购退货单
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增  S.过帐</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="ID">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetPurchaseReturn(string programJobNo, string scanType, string status, string[] docNo, string ID, string siteNo);
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-3-27</createDate>
//<IssueNo>P001-170316001</IssueNo>
//<description>依条码获取送货单明细服务接口</description>
//----------------------------------------------------------------

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business
{
    [TypeKeyOnly]
    [Description("依条码获取送货单明细服务 接口")]
    public interface IGetFILPurchaseArrivalBcodeService
    {
        /// <summary>
        /// 依条码获取送货单明细服务
        /// </summary>
        /// <param name="programJobNo">作业编号  1-1采购收货(采购单号),3-1收货入库（采购单号）</param>
        /// <param name="scanType">扫描类型1.单据条码 2.组合条码</param>
        /// <param name="status">执行动作A.新增  S.过帐</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetFILPurchaseArrivalBcode(string programJobNo, string scanType, string status,
            string[] docNo, string siteNo);
    }
}
//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-3-27</createDate>
//<IssueNo>P001-170316001</IssueNo>
//<description>获取待到货订单通知服务接口</description>
//----------------------------------------------------------------
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("获取待到货订单通知服务 接口")]
    public interface IGetFILPurchaseArrivalListService {
        /// <summary>
        /// 查询待到货订单
        /// </summary>
        /// <param name="programJobNo">作业编号  1-1采购收货,3-1收货入库</param>
        /// <param name="scanType">扫描类型1.有条码 2.无条码</param>
        /// <param name="status">执行动作A.新增 S.過帳 Y.審核</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="Condition">多笔记录，包含两个属性一、项次//1.单号 2.日期  3.人员 4.部门 5.供货商6. 客户 7.料号 二、栏位值</param>
        /// <returns></returns>
        DependencyObjectCollection GetFILPurchaseArrivalList(string programJobNo, string scanType, string status, string siteNo, DependencyObjectCollection condition);
    }
}

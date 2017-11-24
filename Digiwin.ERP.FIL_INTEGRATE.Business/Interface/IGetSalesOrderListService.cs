//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>liwei1</author>
//<createDate>2017/07/19</createDate>
//<IssueNo>P001-170717001</IssueNo>
//<description>获取寄售订单通知服务接口</description>
//----------------------------------------------------------------

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取寄售订单通知服务接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取寄售订单通知服务接口")]
    public interface IGetSalesOrderListService {
        /// <summary>
        /// 根据传入的条码，获取相应的寄售订单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.有条码 2.无条码</param>
        /// <param name="status">执行动作A.新增  S.过帐</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="condition">/多笔记录，包含两个属性①seq   numeric  项次//1.单号 2.日期  3.人员 4.部门 5.供货商6. 客户 7.料号②value  String 栏位值</param>
        /// <returns></returns>
        DependencyObjectCollection GetSalesOrderList(string programJobNo, string scanType, string status, string siteNo, DependencyObjectCollection condition);
    }
}

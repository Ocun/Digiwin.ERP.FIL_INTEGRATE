//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>liwei1</author>
//<createDate>2017/07/19</createDate>
//<IssueNo>P001-170717001</IssueNo>
//<description>获取寄售订单服务接口</description>

using Digiwin.Common;
using Digiwin.Common.Torridity;
using System.ComponentModel;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {

    [TypeKeyOnly]
    [Description("获取寄售订单服务接口")]
    public interface IGetSalesOrderService {

        /// <summary>
        /// 根据传入的条码，获取相应的寄售订单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="scanType">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂</param>
        /// <returns></returns>
        DependencyObjectCollection GetSalesOrder(string programJobNo, string status, string scanType, string[] docNo, string id, string siteNo);
    }
}
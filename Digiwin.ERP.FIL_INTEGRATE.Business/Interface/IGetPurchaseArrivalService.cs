//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-04</createDate>
//<description>获取到货单服务 接口</description>
//---------------------------------------------------------------- 
//20161216 modi by liwei1 for P001-161215001 逻辑调整
using System.ComponentModel;
using System.Data;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///获取到货单服务 接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取到货单服务 接口")]
    public interface IGetPurchaseArrivalService {

        /// <summary>
        /// 查询到货单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetPurchaseArrival(string programJobNo, string scanType, string status, string[] docNo, string id, string siteNo);//20161216 add by liwei1 for P001-161215001
        //DependencyObjectCollection GetPurchaseArrival(string programJobNo, string scanType, string status, string docNo, string id, string siteNo);//20161216 mark by liwei1 for P001-161215001
    }
}
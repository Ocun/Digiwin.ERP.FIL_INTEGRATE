//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-03</createDate>
//<description>获取销货单服务接口</description>
//---------------------------------------------------------------- 
//20161216 modi by liwei1 for P001-161215001 逻辑调整
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取销货单服务接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取销货单服务接口")]
    public interface IGetSalesDeliveryService {

        /// <summary>
        /// 根据传入的条码，获取相应的销货单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="scanType">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="siteNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="site_no">工厂</param>
        /// <returns></returns>
        DependencyObjectCollection GetSalesDelivery(string programJobNo, string status, string scanType, string[] docNo, string id, string siteNo);//20161216 add by liwei1 for P001-161215001
        //DependencyObjectCollection GetSalesDelivery(string programJobNo, string status, string scanType, string docNo, string id, string siteNo);//20161216 mark by liwei1 for P001-161215001
    }
}
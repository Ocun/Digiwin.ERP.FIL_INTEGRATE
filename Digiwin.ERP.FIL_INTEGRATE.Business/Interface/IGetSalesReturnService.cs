//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2017-02-13</createDate>
//<description>获取销退单服务 接口</description>
//---------------------------------------------------------------- 

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business{
    /// <summary>
    /// 获取销退单服务 接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取销退单服务 接口")]
    public interface IGetSalesReturnService{
        /// <summary>
        /// 获取销退单服务
        /// </summary>
        /// <param name="programJobNo">作业编号  6.销退单</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作  A.新增  S.过帐</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetSalesReturn(string programJobNo, string scanType, string status, string[] docNo,string id, string siteNo);
    }
}
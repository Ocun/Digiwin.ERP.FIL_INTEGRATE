//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2017-02-13</createDate>
//<description>获取销退单通知服务 接口</description>
//---------------------------------------------------------------- 
//20170328 modi by wangyq for P001-170327001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取销退单通知服务 接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取出入单通知服务 接口")]
    public interface IGetSalesReturnListService {
        /// <summary>
        /// 获取销退单通知
        /// </summary>
        /// <param name="programJobNo">作业编号  6.销退单</param>
        /// <param name="scanType">扫描类型  1.有条码 2.无条码</param>
        /// <param name="status">执行动作  A.新增  S.过帐</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetSalesReturnList(string programJobNo, string scanType, string status, string siteNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            );
    }
}
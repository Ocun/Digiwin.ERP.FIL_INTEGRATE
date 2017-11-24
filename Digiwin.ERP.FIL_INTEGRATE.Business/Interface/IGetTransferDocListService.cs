//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-04</createDate>
//<description>获取调出单通知服务 接口</description>
//---------------------------------------------------------------- 
//20170328 modi by wangyq for P001-170327001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取调出单通知服务 接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取调出单通知服务 接口")]
    public interface IGetTransferDocListService {

        /// <summary>
        /// 查询调出单
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetTransferDoc(string programJobNo, string scanType, string status, string siteNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            );
    }
}
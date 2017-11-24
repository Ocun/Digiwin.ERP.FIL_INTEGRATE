//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-03</createDate>
//<description>获取入库工单服务 接口</description>
//---------------------------------------------------------------- 

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取入库工单服务 接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取入库工单服务 接口")]
    public interface IGetMOProductMoService {

        /// <summary>
        /// 查询工单产出信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetMOProduct(string programJobNo, string scanType, string status, string[] docNo, string id, string siteNo);  //20170726 modi by shenbao for P001-170717001 修改docNo为数组
    }
}
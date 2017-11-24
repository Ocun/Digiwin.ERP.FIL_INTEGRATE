//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-04</createDate>
//<description>获取销货单通知服务接口</description>
//----------------------------------------------------------------
//20170328 modi by wangyq for P001-170327001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取销货单通知服务接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取销货单通知服务接口")]
    public interface IGetSalesDeliveryListService {
        /// <summary>
        /// 根据传入的条码，获取相应的销货单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.有条码 2.无条码</param>
        /// <param name="status">执行动作A.新增  S.过帐</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetSalesDeliveryList(string programJobNo, string scanType, string status, string siteNo
             , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            );
    }
}

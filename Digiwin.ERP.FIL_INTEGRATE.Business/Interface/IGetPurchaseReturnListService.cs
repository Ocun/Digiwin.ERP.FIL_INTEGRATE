//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-02-10</createDate>
//<IssueNo>P001-170207001</IssueNo>
//<description>获取到货单通知服务 接口</description>
//---------------------------------------------------------------- 
//20170328 modi by wangyq for P001-170327001

using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("获取采购退货单通知服务接口")]
    public interface IGetPurchaseReturnListService {
        /// <summary>
        /// 获取采购退货单通知
        /// </summary>
        /// <param name="programJobNo">作业编号  4-1.采购退货单</param>
        /// <param name="scanType">扫描类型  1.有条码 2.无条码</param>
        /// <param name="status">执行动作  A.新增  S.过帐</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        DependencyObjectCollection GetPurchaseReturnList(string programJobNo, string scanType, string status, string siteNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            );
    }
}

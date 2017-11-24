//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/9/25 16:58:20</CreateDate>
//<IssueNO>P001-170717001</IssueNO>
//<Description>获取销货出库单通知服务</Description>
//----------------------------------------------------------------  

using System;
using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///  
    /// </summary>
    [TypeKeyOnly]
    [Description("")]
    public interface IGetSalesIssueListService {
        /// <summary>
        /// 获取销货出库单通知服务
        /// </summary>
        /// <param name="programJobNo"></param>
        /// <param name="scanType"></param>
        /// <param name="status"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        DependencyObjectCollection GetSalesIssueList(string programJobNo, string scanType, string status, string siteNo, DependencyObjectCollection condition);
    }
}

//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/9/25 16:58:20</CreateDate>
//<IssueNO>P001-170717001</IssueNO>
//<Description>获取销货出库单服务</Description>
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
    public interface IGetSalesIssueService {
        /// <summary>
        /// 获取销货出库单服务
        /// </summary>
        /// <param name="programJobNo"></param>
        /// <param name="scanType"></param>
        /// <param name="status"></param>
        /// <param name="docNo"></param>
        /// <param name="siteNo"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        DependencyObjectCollection GetSalesIssue(string programJobNo, string scanType, string status, string[] docNo, string siteNo, string ID);
    }
}

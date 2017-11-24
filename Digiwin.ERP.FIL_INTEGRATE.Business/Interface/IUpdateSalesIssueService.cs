//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/9/5 14:27:20</CreateDate>
//<IssueNO>P001-170717001</IssueNO>
//<Description>更新销货出库单服务</Description>
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
    public interface IUpdateSalesIssueService {
        DependencyObjectCollection UpdateSalesIssue(string employeeNo, string scanType, DateTime reportDatetime, string pickingDepartmentNo
                  , string recommendedOperations, string recommendedFunction, string scanDocNo, DependencyObjectCollection collection);
    }
}

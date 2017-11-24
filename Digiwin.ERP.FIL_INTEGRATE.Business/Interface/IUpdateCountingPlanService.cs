//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/2/7 16:35:19</CreateDate>
//<IssueNO>B001-170206023</IssueNO>
//<Description>更新盘点计划服务接口定义</Description>
//----------------------------------------------------------------  

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///  
    /// </summary>
    [TypeKeyOnly]
    [Description("")]
    public interface IUpdateCountingPlanService {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="counting_type">盘点类型</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="counting_no">盘点计划编号</param>
        /// <param name="scan"></param>
        void UpdateCountingPlan(string counting_type, string site_no, string counting_no, DependencyObjectCollection scan);
    }
}

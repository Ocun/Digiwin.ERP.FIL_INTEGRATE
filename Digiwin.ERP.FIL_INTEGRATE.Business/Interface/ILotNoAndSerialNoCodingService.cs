//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/1/3 14:55:18</CreateDate>
//<IssueNO>P001-161215001</IssueNO>
//<Description>移动应用自动产生批号服务接口</Description>
//----------------------------------------------------------------  

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///  
    /// </summary>
    [TypeKeyOnly]
    [Description("")]
    public interface ILotNoAndSerialNoCodingService {
        /// <summary>
        /// 移动应用自动产生批号服务
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">执行动作</param>
        /// <param name="item_no">料件编号</param>
        /// <param name="item_feature_no">产品特征</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="object_no">供货商编号//作業編號=1.2.3為必要輸入</param>
        /// <param name="action">动作:Q.查询 I.新增</param>
        /// <param name="lot_no">批号:当动作=Q时，为必输</param>
        /// <returns></returns>
        Hashtable LotNoAndSerialNoCoding(string program_job_no, string status, string item_no, string item_feature_no, string site_no, string object_no, string action, string lot_no);
    }
}

//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/1/19 10:55:13</CreateDate>
//<IssueNO>P001-170118001 </IssueNO>
//<Description>更新批号服务接口</Description>
//----------------------------------------------------------------

using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///  
    /// </summary>
    [TypeKeyOnly]
    [Description("")]
    public interface IUpdateLotService {

        /// <summary>
        /// 更新批号
        /// </summary>
        /// <param name="item_no">料件编号</param>
        /// <param name="item_feature_no">产品特征</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="lot_no">批号</param>
        /// <param name="lot_description">批号说明</param>
        /// <param name="effective_date">生效日期</param>
        /// <param name="effective_deadline">有效截止日</param>
        /// <param name="remarks">备注</param>
        /// <returns></returns>
        void UpdateLot(string item_no, string item_feature_no, string site_no, string lot_no, string lot_description, string effective_date, string effective_deadline, string remarks);
    }
}

//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/1/19 10:54:50</CreateDate>
//<IssueNO>P001-170118001 </IssueNO>
//<Description>更新批号服务实现</Description>
//----------------------------------------------------------------  
//20170228 modi by shenbao for B001-170206007

using System;
using System.Collections.Generic;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    /// <summary>
    ///  
    /// </summary>
    [ServiceClass(typeof(IUpdateLotService))]
    [Description("")]
    sealed class UpdateLotService : ServiceComponent, IUpdateLotService {

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
        public void UpdateLot(string item_no, string item_feature_no, string site_no, string lot_no, string lot_description, string effective_date, string effective_deadline, string remarks) {
            QueryNode selectNode = OOQL.Select(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                                .From("ITEM_LOT", "ITEM_LOT")
                                .InnerJoin("ITEM", "ITEM")
                                .On(OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"));
            QueryConditionGroup conditionGroup = OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateConstants(item_no, GeneralDBType.String)
                                       & OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateConstants(lot_no, GeneralDBType.String);
            DateTime dtEffection = effective_date.ToDate();
            DateTime dtInEffection = effective_deadline.ToDate();
            if (!string.IsNullOrEmpty(item_feature_no)) {
                conditionGroup &= OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE") == OOQL.CreateConstants(item_feature_no, GeneralDBType.String);
                //20170228 mark by shenbao for B001-170206007 将这里的语句往下放 ===begin ===
                //selectNode = ((JoinOnNode)selectNode).InnerJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                //                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID")
                //                    & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                //                .Where(OOQL.AuthFilter("ITEM_LOT", "ITEM_LOT") & (conditionGroup));
                //20170228 mark by shenbao for B001-170206007 ===end ===
            }
            //20170228 add by shenbao for B001-170206007 ===begin ===
            selectNode = ((JoinOnNode)selectNode).LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")  //20170228 modi by shenbao for B001-170206007 InnerJoin==>LeftJoin
                                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID")
                                    & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                                .Where(OOQL.AuthFilter("ITEM_LOT", "ITEM_LOT") & (conditionGroup));
            //20170228 add by shenbao for B001-170206007 ===end ===
            List<SetItem> updateList = new List<SetItem>();
            updateList.Add(new SetItem(OOQL.CreateProperty("LOT_DESCRIPTION"), OOQL.CreateConstants(lot_description, GeneralDBType.String)));
            updateList.Add(new SetItem(OOQL.CreateProperty("EFFECTIVE_DATE"), OOQL.CreateConstants(dtEffection, GeneralDBType.Date)));
            updateList.Add(new SetItem(OOQL.CreateProperty("INEFFECTIVE_DATE"), OOQL.CreateConstants(dtInEffection, GeneralDBType.Date)));
            updateList.Add(new SetItem(OOQL.CreateProperty("REMARK"), OOQL.CreateConstants(remarks, GeneralDBType.String)));
            QueryNode node = OOQL.Update("ITEM_LOT").Set(updateList.ToArray())
                .From(selectNode, "selectNode")
                .Where(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("selectNode.ITEM_LOT_ID"));
            this.GetService<IQueryService>().ExecuteNoQueryWithManageProperties(node);
        }
    }
}

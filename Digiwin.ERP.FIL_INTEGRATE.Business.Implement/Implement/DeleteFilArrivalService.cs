//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-12</createDate>
//<description>删除送货单服务</description>
//---------------------------------------------------------------- 
//20170508 modi by liwei1 for P001-161209002

using System;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    [ServiceClass(typeof(IDeleteFilArrivalService))]
    [Description("删除送货单服务")]
    public class DeleteFilArrivalService : ServiceComponent, IDeleteFilArrivalService {

        /// <summary>
        /// 根据传入的单号信息删除送货单
        /// </summary>
        /// <param name="delivery_no">送货单号</param>
        public void DeleteFilArrival(string delivery_no) {
            try {
                //20170508 add by liwei1 for P001-161209002 ---begin---
                //检查该送货单是否已生成到货单，若已生成到货单则不能审核
                if (GetCountNum(delivery_no) > 0) {
                    IInfoEncodeContainer infoEnCode = GetService<IInfoEncodeContainer>();
                    throw new BusinessRuleException(infoEnCode.GetMessage("A111446"));//该送货单已生成到货单，不能删除！
                }
                //20170508 add by liwei1 for P001-161209002 ---end---

                using (ITransactionService transActionService = GetService<ITransactionService>()) {
                    //删除送货单
                    QueryNode queryNode = OOQL.Delete("FIL_ARRIVAL")
                                                                    .Where((OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO") == OOQL.CreateConstants(delivery_no)));
                    GetService<IQueryService>().ExecuteNoQuery(queryNode);

                    //删除送货单单身
                    queryNode = OOQL.Delete("FIL_ARRIVAL.FIL_ARRIVAL_D")
                                                .Where(OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateConstants(delivery_no));
                    GetService<IQueryService>().ExecuteNoQuery(queryNode);
                    transActionService.Complete();
                }
            } catch (Exception) {
                throw;
            }
        }

        //20170508 add by liwei1 for P001-161209002 ---begin---
        /// <summary>
        /// 获取送货单单身存在到货单主键记录数
        /// </summary>
        /// <param name="deliveryNo"></param>
        /// <returns></returns>
        private int GetCountNum(string deliveryNo) {
            QueryNode node =
                OOQL.Select(
                            Formulas.Count(OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_D_ID"), "COUNT_NUM"))
                        .From("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                        .Where((OOQL.AuthFilter("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D"))
                               & ((OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateConstants(deliveryNo))
                               & (OOQL.CreateProperty("FIL_ARRIVAL_D.PURCHASE_ARRIVAL_ID") !=OOQL.CreateConstants(Maths.GuidDefaultValue()))
                               & (OOQL.CreateProperty("FIL_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID") !=OOQL.CreateConstants(Maths.GuidDefaultValue()))));
            return GetService<IQueryService>().ExecuteScalar(node).ToInt32();
        }
        //20170508 add by liwei1 for P001-161209002 ---end---
    }
}

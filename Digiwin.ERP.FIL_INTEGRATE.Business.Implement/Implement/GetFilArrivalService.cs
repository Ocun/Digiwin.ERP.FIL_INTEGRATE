//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-15</createDate>
//<description>送货单号获取服务</description>
//---------------------------------------------------------------- 
//20170619 modi by zhangcn for P001-170606002

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 送货单号获取服务
    /// </summary>
    [ServiceClass(typeof(IGetFilArrivalService))]
    [Description("送货单号获取服务")]
    public class GetFilArrivalService : ServiceComponent, IGetFilArrivalService {
        /// <summary>
        /// 根据传入的供应商等信息获取送货单单号
        /// </summary>
        /// <param name="supplier_no">供应商编号</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <returns></returns>
        public Hashtable GetFilArrival(string supplier_no, string enterprise_no, string site_no) {
            try {
                //查询工厂的其他相关信息
                QueryNode queryNode = GetDeliveryNo(supplier_no);
                DependencyObjectCollection deliveryNos = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //组织返回结果
                Hashtable result = new Hashtable();
                result.Add("item_barcode_detail", deliveryNos);
                return result;
            } catch (Exception) {
                throw;
            }
        }

        /// <summary>
        /// 根据传入的工厂编号，查询工厂的其他相关信息
        /// </summary>
        /// <param name="site_no">门店编号</param>
        /// <returns></returns>
        private QueryNode GetDeliveryNo(string supplierNo) {
            //上个月第一天
            DateTime dateTime = DateTime.Today.AddMonths(-1).ToDate();
            dateTime = dateTime.AddDays(-dateTime.Day + 1);

            return OOQL.Select(OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO", "delivery_no"),
                OOQL.CreateProperty("FIL_ARRIVAL.STATUS", "delivery_stus"),//20170619 add by zhangcn for P001-170606002
                Formulas.IsNull(OOQL.CreateProperty("FIL_ARRIVAL_D.SequenceNumber"), OOQL.CreateConstants(0,GeneralDBType.Decimal), "delivery_seq"),//20170619 add by zhangcn for P001-170606002
                Formulas.IsNull(OOQL.CreateProperty("FIL_ARRIVAL_D.UNARR_QTY"), OOQL.CreateConstants(0, GeneralDBType.Decimal), "unpaid_qty"),//20170619 add by zhangcn for P001-170606002
                Formulas.IsNull(OOQL.CreateProperty("FIL_ARRIVAL_D.RECEIPT_QTY"), OOQL.CreateConstants(0, GeneralDBType.Decimal), "receipt_qty"),//20170619 add by zhangcn for P001-170606002
                Formulas.IsNull(OOQL.CreateProperty("FIL_ARRIVAL_D.STATUS"), OOQL.CreateConstants(string.Empty), "delivery_sub_stus")//20170619 add by zhangcn for P001-170606002
                )
                       .From("FIL_ARRIVAL", "FIL_ARRIVAL")
                       .LeftJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")//20170619 add by zhangcn for P001-170606002
                       .On(OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID") == OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID"))//20170619 add by zhangcn for P001-170606002
                       .Where(OOQL.AuthFilter("FIL_ARRIVAL", "FIL_ARRIVAL")//20170619 add by zhangcn for P001-170606002
                        & (OOQL.CreateProperty("FIL_ARRIVAL.COMPANY_CODE") == OOQL.CreateConstants(supplierNo))
                        & (OOQL.CreateProperty("FIL_ARRIVAL.STATUS") == OOQL.CreateConstants("3"))
                        & (OOQL.CreateProperty("FIL_ARRIVAL.DOC_DATE") <= Formulas.GetDate())
                        & (OOQL.CreateProperty("FIL_ARRIVAL.DOC_DATE") >= OOQL.CreateConstants(dateTime)));
        }
    }
}

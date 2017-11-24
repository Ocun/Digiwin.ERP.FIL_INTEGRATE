//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-03-24</createDate>
//<description>获取送货单明细服务</description>
//---------------------------------------------------------------- 
//20170509 modi by liwei1 for P001-161209002
// modi by 08628 for P001-171023001
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取送货单明细服务
    /// </summary>
    [ServiceClass(typeof(IGetFILPurchaseArrivalService))]
    [Description("获取送货单明细服务")]
    public class GetFILPurchaseArrivalService : ServiceComponent, IGetFILPurchaseArrivalService {

        /// <summary>
        /// 获取送货单明细
        /// </summary>
        /// <param name="delivery_no">送货单号</param>
        /// <returns></returns>
        public Hashtable GetFilPurchaseArrival(string delivery_no) {
            try {
                #region 参数检查
                if (Maths.IsEmpty(delivery_no)) {
                    var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "delivery_no"));//‘入参【delivery_no】未传值’
                }
                #endregion
                //组织返回结果
                Hashtable result = new Hashtable();

                //获取送货单单号\送货日期\供应商名称
                QueryNode node = QueryFilArrival(delivery_no);
                DependencyObjectCollection filArrival = GetService<IQueryService>().ExecuteDependencyObject(node);
                if (filArrival.Count > 0) {
                    //单头能查询到数据再查询对应单身信息
                    node = QueryFilArrivalD(delivery_no);
                    //获取送货单明细
                    DependencyObjectCollection deliveryDetail = GetService<IQueryService>().ExecuteDependencyObject(node);

                    result.Add("delivery_no", filArrival[0]["delivery_no"]);//送货单号
                    result.Add("date", filArrival[0]["date"].ToDate().ToString("d"));//送货日期
                    result.Add("supplier_name", filArrival[0]["supplier_name"]);//供应商名称
                    result.Add("delivery_detail", deliveryDetail);
                } else {
                    //查询不到数据的时候返回空值及空结果集
                    result.Add("delivery_no", string.Empty);//送货单号
                    result.Add("date", OrmDataOption.EmptyDateTime.ToString("d"));//送货日期
                    result.Add("supplier_name", string.Empty);//供应商名称

                    DependencyObjectType deliveryDt = new DependencyObjectType("delivery_detail");
                    DependencyObjectCollection deliveryDetail = new DependencyObjectCollection(deliveryDt);
                    result.Add("delivery_detail", deliveryDetail);
                }
                return result;
            } catch (System.Exception) {
                throw;
            }
        }

        #region 数据库相关
        /// <summary>
        /// 查询送货单数据
        /// </summary>
        /// <param name="deliveryNo">送货单单号</param>
        /// <returns></returns>
        private QueryNode QueryFilArrival(string deliveryNo) {
            return OOQL.Select(true,
                                    OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO", "delivery_no"),
                                    OOQL.CreateProperty("FIL_ARRIVAL.DOC_DATE", "date"),
                                    OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_FULL_NAME", "supplier_name"))
                                .From("FIL_ARRIVAL", "FIL_ARRIVAL")
                                .InnerJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                                .On((OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID") == OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID")))
                                .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                                .On((OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO")))
                                .Where((OOQL.AuthFilter("FIL_ARRIVAL", "FIL_ARRIVAL"))
                                    & (OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO") == OOQL.CreateConstants(deliveryNo)));
        }

        /// <summary>
        /// 查询送货单明细
        /// </summary>
        /// <param name="deliveryNo">送货单单号</param>
        /// <returns></returns>
        private QueryNode QueryFilArrivalD(string deliveryNo)
        {
            return OOQL.Select(true,
                OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE", "item_no"),
                OOQL.CreateProperty("FIL_ARRIVAL_D.UNIT_CODE", "unit_no"),
                OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_NAME", "item_name"),
                OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_DESCRIPTION", "item_spec"),
                OOQL.CreateProperty("FIL_ARRIVAL_D.WAREHOUSE_CODE", "warehouse_no"),
                OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE", "item_feature_no"),
                OOQL.CreateProperty("QTY.ACTUAL_QTY", "qty"),
                // ADD by 08628 for P001-171023001 b
                OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_CODE", "main_warehouse_no"),
                OOQL.CreateProperty("MAIN_BIN.BIN_CODE", "main_storage_no"),
                OOQL.CreateConstants(string.Empty, "first_in_first_out_control")
                // ADD by 08628 for P001-171023001 e
                )
                .From("FIL_ARRIVAL", "FIL_ARRIVAL")
                .InnerJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                .On((OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID") ==
                     OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID")))
                .LeftJoin(
                    OOQL.Select(
                        OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO", "DOC_NO"),
                        OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE", "ITEM_CODE"),
                        OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE", "ITEM_FEATURE_CODE"),
                        Formulas.Sum(
                            OOQL.CreateProperty("FIL_ARRIVAL_D.ACTUAL_QTY"), "ACTUAL_QTY"))
                        .From("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                        .GroupBy(
                            OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO"),
                            OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE"),
                            OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE")), "QTY")
                .On((OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateProperty("QTY.DOC_NO"))
                    & (OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE") == OOQL.CreateProperty("QTY.ITEM_CODE"))
                    &
                    (OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE") ==
                     OOQL.CreateProperty("QTY.ITEM_FEATURE_CODE")))
                // add by 08628 for P001-171023001
                .LeftJoin("PLANT", "PLANT")
                .On(OOQL.CreateProperty("FIL_ARRIVAL_D.PLANT_CODE") ==
                    OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .LeftJoin("ITEM", "ITEM")
                .On(OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    &
                    OOQL.CreateProperty("PLANT.PLANT_ID") ==
                    OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") ==
                    OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") ==
                    OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                    & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                // add by 08628 for P001-171023001

                .Where((OOQL.AuthFilter("FIL_ARRIVAL", "FIL_ARRIVAL"))
                       & (OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO") == OOQL.CreateConstants(deliveryNo))
                       & (OOQL.CreateProperty("FIL_ARRIVAL_D.STATUS") == OOQL.CreateConstants("N"))
                //20170509 add by liwei1 for P001-161209002                                                                  
                );
        }
        #endregion
    }
}

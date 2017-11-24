//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-27</createDate>
//<description>获取采购未交明细服务</description>
//---------------------------------------------------------------- 
//20170504 modi by wangyq for P001-161209002
//20170619 modi by zhangcn for B001-170629006
//20170903 modi by liwei1 for B001-170901012
//20170919 modi by liwei1 for B001-170918003 
//20171010 modi by zhangcn for B001-171010004	回传增加特征码规格

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取采购未交明细服务
    /// </summary>
    [ServiceClass(typeof(IPurchaseOrderUaqtyService))]
    [Description("获取采购未交明细服务")]
    public class PurchaseOrderUaqtyService : ServiceComponent, IPurchaseOrderUaqtyService {
        /// <summary>
        /// 根据传入的供应商查询出所有的采购未交明细
        /// </summary>
        /// <param name="supplier_no">供应商编号(必填)</param>
        /// <param name="date_s">订单日期起</param>
        /// <param name="date_e">订单日期止</param>
        /// <param name="due_date_s">预交日期起</param>
        /// <param name="due_date_e">预交日期止</param>
        /// <param name="item_no">料件编号</param>
        /// <param name="item_name">品名</param>
        /// <param name="item_spec">规格</param>
        /// <param name="qc_type">检验否</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <param name="purchase_no">采购单号</param>
        /// <returns></returns>
        public Hashtable PurchaseOrderUaqty(string supplier_no, string date_s, string date_e, string due_date_s, string due_date_e,
            string item_no, string item_name, string item_spec, string qc_type, string enterprise_no, string site_no
            , string purchase_no//20170919 add by liwei1 for B001-170918003 
            ) {
            try {
                if (Maths.IsEmpty(supplier_no)) {
                    IInfoEncodeContainer infoEnCode = GetService<IInfoEncodeContainer>();
                    throw new BusinessRuleException(infoEnCode.GetMessage("A111201", "supplier_no"));//‘入参【supplier_no】未传值’( A111201)
                }
                //查询采购未交明细
                QueryNode queryNode = GetPurchaseDetail(supplier_no, date_s, date_e, due_date_s, due_date_e, item_no, item_name, item_spec, site_no
                    , purchase_no//20170919 add by liwei1 for B001-170918003 
                    );
                DependencyObjectCollection purchaseDetail = GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //组合返回结果
                Hashtable result = new Hashtable { { "purchase_detail", purchaseDetail } };
                return result;
            } catch (Exception) {
                throw;
            }
        }

        private QueryNode GetPurchaseDetail(string supplierNo, string dateS, string dateE, string dueDateS, string dueDateE,
            string itemNo, string itemName, string itemSpec, string siteNo
            , string purchase_no//20170919 add by liwei1 for B001-170918003             
            ){
            QueryNode subNodeFilArrival = GetFilArrivalQueryNode();
            JoinOnNode joinOnNode =
                OOQL.Select(
                            OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "purchase_no"),
                            OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_DATE", "purchase_date"),
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.PLAN_ARRIVAL_DATE", "so_due_date"),
                            OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber", "seq"),
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber", "line_seq"),
                            OOQL.CreateConstants(0, GeneralDBType.Int32, "batch_seq"),
                            OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                            OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_DESCRIPTION", "item_name"),
                            OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_SPECIFICATION", "item_spec"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_no"),//20170504 add by wangyq for P001-161209002
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_name"),//20171010 modi by zhangcn for B001-171010004
                            Formulas.IsNull(
                                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "unit_no"),
                            OOQL.CreateConstants("N",GeneralDBType.String, "qc_type"),//20170619 modi by zhangcn for B001-170629006 【OLD：null】
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.BUSINESS_QTY", "purchase_qty"),
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.ARRIVED_BUSINESS_QTY", "receipt_qty"),
                            //OOQL.CreateArithmetic(
                            //        OOQL.CreateProperty("PURCHASE_ORDER_SD.BUSINESS_QTY"),
                            //        OOQL.CreateProperty("PURCHASE_ORDER_SD.ARRIVED_BUSINESS_QTY"), ArithmeticOperators.Sub,"unpaid_qty"),

                            OOQL.CreateArithmetic(OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ORDER_SD.BUSINESS_QTY"),OOQL.CreateProperty("PURCHASE_ORDER_SD.ARRIVED_BUSINESS_QTY"), ArithmeticOperators.Sub),
                                                  Formulas.IsNull(OOQL.CreateProperty("FILARR.AQTY"),OOQL.CreateConstants(0)), 
                                                  ArithmeticOperators.Sub, 
                                                  "unpaid_qty"),
                            Formulas.IsNull(OOQL.CreateProperty("FILARR.AQTY"),OOQL.CreateConstants(0), "on_the_way_qty"),//20170619 modi by zhangcn for B001-170629006 【OLD：0】
                            Formulas.IsNull(
                                    OOQL.CreateProperty("ITEM_PURCHASE.RECEIPT_OVER_RATE") * OOQL.CreateConstants(100, GeneralDBType.Decimal),
                                    OOQL.CreateConstants(0, GeneralDBType.Decimal), "over_deliver_rate"),
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.REMARK", "remark"),
                            Formulas.IsNull(
                                    OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "employee_name"),
                            Formulas.IsNull(
                                    OOQL.CreateProperty("PRICE_UNIT.UNIT_CODE"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "valuation_unit_no"),
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.PRICE_QTY", "valuation_qty"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "returned_qty"),//20170619 add by zhangcn for B001-170629006
                            OOQL.CreateConstants(1, GeneralDBType.Decimal, "box_qty"))//20170619 add by zhangcn for B001-170629006
                        .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                        .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                        .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")))
                        .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                        .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")))
                        .InnerJoin("PLANT", "PLANT")
                        .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                        .InnerJoin("ITEM", "ITEM")
                        .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")))
                //20170504 add by wangyq for P001-161209002  ========================begin=========================
                        .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                        .On(OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                //20170504 add by wangyq for P001-161209002  ========================end=========================
                        .LeftJoin("UNIT", "UNIT")
                        .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID")))
                        .InnerJoin("SUPPLIER", "SUPPLIER")
                        .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID")))
                        .LeftJoin("ITEM_PURCHASE", "ITEM_PURCHASE")
                        .On((OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                            & (OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid") == OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid")))
                        .LeftJoin("EMPLOYEE", "EMPLOYEE")
                        .On((OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("PURCHASE_ORDER.Owner_Emp")))
                        .LeftJoin("UNIT", "PRICE_UNIT")
                        .On((OOQL.CreateProperty("PRICE_UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PRICE_UNIT_ID")))
                //20170619 add by zhangcn for B001-170629006 ===begin===
                        .LeftJoin(subNodeFilArrival, "FILARR")
                        .On(OOQL.CreateProperty("FILARR.ORDER_NO") == OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") &
                            OOQL.CreateProperty("FILARR.ORDER_SE") == OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber") &
                            OOQL.CreateProperty("FILARR.ORDER_SE_SE") == OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber"));
                //20170619 add by zhangcn for B001-170629006 ===begin===

            //如果起始日期为空格，空值，null时默认最小日期
            if (Maths.IsEmpty(dateS.ToDate())) {
                dateS = OrmDataOption.EmptyDateTime.ToStringExtension();
            }
            if (Maths.IsEmpty(dueDateS.ToDate())) {
                dueDateS = OrmDataOption.EmptyDateTime.ToStringExtension();
            }
            //如果结束日期为空格，空值，null时默认最大日期
            if (Maths.IsEmpty(dateE.ToDate())) {
                dateE = OrmDataOption.EmptyDateTime1.ToStringExtension();
            }
            if (Maths.IsEmpty(dueDateE.ToDate())) {
                dueDateE = OrmDataOption.EmptyDateTime1.ToStringExtension();
            }

            QueryConditionGroup conditionGroup = (OOQL.AuthFilter("TRANSACTION_DOC", "TRANSACTION_DOC"))
                                        & ((OOQL.CreateProperty("PURCHASE_ORDER.ApproveStatus") == OOQL.CreateConstants("Y"))
                                        //& (OOQL.CreateProperty("PURCHASE_ORDER_SD.BUSINESS_QTY") > OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIPTED_BUSINESS_QTY"))//20170903 mark by liwei1 for B001-170901012
                                        & (OOQL.CreateProperty("PURCHASE_ORDER_SD.BUSINESS_QTY") > OOQL.CreateProperty("PURCHASE_ORDER_SD.ARRIVED_BUSINESS_QTY"))//20170903 add by liwei1 for B001-170901012
                                        & (OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateConstants(supplierNo))
                                        & ((OOQL.CreateProperty("PURCHASE_ORDER.DOC_DATE") >= OOQL.CreateConstants(dateS.ToDate()))
                                           & (OOQL.CreateProperty("PURCHASE_ORDER.DOC_DATE") <= OOQL.CreateConstants(dateE.ToDate())))
                                        & ((OOQL.CreateProperty("PURCHASE_ORDER_SD.PLAN_ARRIVAL_DATE") >= OOQL.CreateConstants(dueDateS.ToDate()))
                                           & (OOQL.CreateProperty("PURCHASE_ORDER_SD.PLAN_ARRIVAL_DATE") <= OOQL.CreateConstants(dueDateE.ToDate()))));
            //如果【品号】不为空增加条件
            if (!Maths.IsEmpty(itemNo)) {
                conditionGroup &= (OOQL.CreateProperty("ITEM.ITEM_CODE").Like(OOQL.CreateConstants("%" + itemNo + "%")));
            }
            //如果【品名】不为空增加条件
            if (!Maths.IsEmpty(itemName)) {
                conditionGroup &= (OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_DESCRIPTION").Like(OOQL.CreateConstants("%" + itemName + "%")));
            }
            //如果【规格】不为空增加条件
            if (!Maths.IsEmpty(itemSpec)) {
                conditionGroup &= (OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_SPECIFICATION").Like(OOQL.CreateConstants("%" + itemSpec + "%")));
            }
            //如果营运中心不为空增加条件
            if (!Maths.IsEmpty(siteNo)) {
                conditionGroup &= (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo));
            }

            //20170919 add by liwei1 for B001-170918003 ===begin===
            //如果采购单号不为空增加条件
            if (!Maths.IsEmpty(purchase_no)) {
                conditionGroup &= (OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateConstants(purchase_no));
            }
            //20170919 add by liwei1 for B001-170918003 ===end===

            return joinOnNode.Where(conditionGroup)
                //20170919 add by liwei1 for B001-170918003 ===begin===
                .OrderBy(
                    OOQL.CreateOrderByItem(
                            OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"), SortType.Asc),
                    OOQL.CreateOrderByItem(
                            OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber"), SortType.Asc))
                //20170919 add by liwei1 for B001-170918003 ===end===
                ;
        }

        //20170619 add by zhangcn for B001-170629006 ===begin===
        private QueryNode GetFilArrivalQueryNode(){
            QueryNode node = OOQL.Select(Formulas.Sum("FIL_ARRIVAL_D.ACTUAL_QTY", "AQTY"),
                OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO", "ORDER_NO"),
                OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE", "ORDER_SE"),
                OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE", "ORDER_SE_SE")
                )
                .From("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                .InnerJoin("FIL_ARRIVAL", "FIL_ARRIVAL")
                .On(OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID") ==
                    OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID"))
                .Where(OOQL.CreateProperty("FIL_ARRIVAL.STATUS") == OOQL.CreateConstants("2") &
                       OOQL.CreateProperty("FIL_ARRIVAL_D.STATUS") == OOQL.CreateConstants("N"))
                .GroupBy(OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO"),
                         OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE"),
                         OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE"));

            return node;
        }
        //20170619 add by zhangcn for B001-170629006 ===begin===
    }
}

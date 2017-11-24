//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-12</createDate>
//<description>对账清单服务</description>
//---------------------------------------------------------------- 
//20170508 modi by liwei1 for P001-161209002
//20171010 modi by zhangcn for B001-171010004 对账单明细体现退货明细
//20171021 add by liwei1 for B001-171020001

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    [ServiceClass(typeof(ICheckListService))]
    [Description("对账清单服务")]
    public class CheckListService : ServiceComponent, ICheckListService {
        /// <summary>
        /// 根据传入的供应商查询出所有的对账清单
        /// </summary>
        /// <param name="supplier_no">供应商编号(必填)</param>
        /// <param name="date_s">对账开始日期</param>
        /// <param name="date_e">对账截止日期</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <returns></returns>
        public Hashtable CheckList(string supplier_no, string date_s, string date_e, string enterprise_no, string site_no) {
            try {
                if (Maths.IsEmpty(supplier_no)) {
                    IInfoEncodeContainer infoEnCode = GetService<IInfoEncodeContainer>();
                    throw new BusinessRuleException(infoEnCode.GetMessage("A111201", "supplier_no"));//‘入参【supplier_no】未传值’( A111201)
                }
                //获取对账清单
                QueryNode queryNode = GetCheckListDetail(supplier_no, date_s, date_e, site_no);
                DependencyObjectCollection checkListDetail = GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //返回结果
                Hashtable result = new Hashtable{{"check_list_detail", checkListDetail}};
                return result;
            } catch (Exception) {
                throw;
            }
        }

        /// <summary>
        /// 根据传入的供应商查询出所有的对账清单数据
        /// </summary>
        /// <param name="supplierNo">供应商</param>
        /// <param name="dateS">对账开始日期</param>
        /// <param name="dateE">对账截止日期</param>
        /// <param name="siteNo">营运中心</param>
        /// <returns></returns>
        private QueryNode GetCheckListDetail(string supplierNo, string dateS, string dateE, string siteNo){
            //如果结束日期为空格，空值，null时默认最大日期
            if (Maths.IsEmpty(dateE.ToDate())) {
                dateE = OrmDataOption.EmptyDateTime1.ToStringExtension();
            }
            //如果开始日期为空格，空值，null时默认最小日期
            if (Maths.IsEmpty(dateS.ToDate())) {
                dateS = OrmDataOption.EmptyDateTime.ToStringExtension();
            }
            JoinOnNode joinOnNode =
                OOQL.Select(
                    OOQL.CreateProperty("PURCHASE_RECEIPT.TRANSACTION_DATE", "create_date"),
                    OOQL.CreateProperty("PURCHASE_RECEIPT.DOC_NO", "stock_in_no"),
                    OOQL.CreateProperty("PURCHASE_RECEIPT.PURCHASE_RECEIPT_D.SequenceNumber", "seq"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                    OOQL.CreateProperty("PURCHASE_RECEIPT_D.ITEM_DESCRIPTION", "item_name"),
                    OOQL.CreateProperty("PURCHASE_RECEIPT_D.ITEM_SPECIFICATION", "item_spec"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty), "item_feature_no"),//20171021 add by liwei1 for B001-171020001
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_feature_name"),//20171021 add by liwei1 for B001-171020001
                    #region //20170508 mark by liwei1 for P001-161209002
                    //OOQL.CreateProperty("PURCHASE_RECEIPT_D.PRICE_QTY", "qty"),
                    //Formulas.IsNull(
                    //    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    //    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "unit_no"),
                    //OOQL.CreateProperty("PAYABLE_DOC_D.DISCOUNTED_PRICE_INTAX", "price"),
                    #endregion
                    //20170508 add by liwei1 for P001-161209002 ---begin---
                    OOQL.CreateProperty("PURCHASE_RECEIPT_D.BUSINESS_QTY", "qty"),
                    Formulas.IsNull(
                        OOQL.CreateProperty("B_UNIT.UNIT_CODE"),
                        OOQL.CreateConstants(" ", GeneralDBType.String), "unit_no"),
                    Formulas.IsNull(
                        OOQL.CreateProperty("UNIT.UNIT_CODE"),
                        OOQL.CreateConstants(" ", GeneralDBType.String), "valuation_unit_no"),
                    OOQL.CreateProperty("PURCHASE_RECEIPT_D.PRICE_QTY", "valuation_qty"),
                    OOQL.CreateProperty("PURCHASE_ARRIVAL_D.DISCOUNTED_PRICE", "price"),
                    //20170508 add by liwei1 for P001-161209002 ---end---
                    OOQL.CreateProperty("PURCHASE_RECEIPT_D.AMOUNT_UNINCLUDE_TAX_OC", "amount"), 
                    OOQL.CreateArithmetic(
                        OOQL.CreateProperty("PURCHASE_RECEIPT_D.AMOUNT_UNINCLUDE_TAX_OC"),
                        OOQL.CreateProperty("PURCHASE_RECEIPT_D.TAX_OC"), ArithmeticOperators.Plus, "tax_amount"),
                    OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "purchase_no"),
                    OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_D.SequenceNumber", "purchase_seq"),
                    #region //20170508 mark by liwei1 for P001-161209002
                    //OOQL.CreateProperty("PAYABLE_DOC_D.PRICE_QTY", "billing_qty"),
                    //(OOQL.CreateArithmetic(
                    //    OOQL.CreateProperty("PURCHASE_RECEIPT_D.PRICE_QTY"),
                    //    OOQL.CreateProperty("PAYABLE_DOC_D.PRICE_QTY"), ArithmeticOperators.Sub, "not_billing_qty")),
                    //OOQL.CreateProperty("PAYABLE_DOC_D.AMT_TC", "billing_amount"),
                    //OOQL.CreateArithmetic((OOQL.CreateProperty("PURCHASE_RECEIPT_D.AMOUNT_UNINCLUDE_TAX_OC")
                    //    + OOQL.CreateProperty("PURCHASE_RECEIPT_D.TAX_OC")),
                    //    OOQL.CreateProperty("PAYABLE_DOC_D.AMT_TC"), ArithmeticOperators.Sub, "not_billing_amount"))
                    #endregion
                    //20170508 add by liwei1 for P001-161209002 ---begin---
                    OOQL.CreateProperty("PURCHASE_RECEIPT_D.SETTLEMENT_PRICE_QTY","billing_qty"),
		            OOQL.CreateArithmetic(
			            OOQL.CreateProperty("PURCHASE_RECEIPT_D.PRICE_QTY"),
			            OOQL.CreateProperty("PURCHASE_RECEIPT_D.SETTLEMENT_PRICE_QTY"),ArithmeticOperators.Sub,"not_billing_qty"),
                    Formulas.Case(null, OOQL.CreateProperty("PURCHASE_RECEIPT_D.SETTLEMENT_AMT_UN_OC")
                        + OOQL.CreateProperty("PURCHASE_RECEIPT_D.SETTLEMENT_TAX_OC"),
                                OOQL.CreateCaseArray(
                                        OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_ARRIVAL.TAX_INCLUDED") == OOQL.CreateConstants(true, GeneralDBType.Boolean)),
                                                OOQL.CreateProperty("PURCHASE_RECEIPT_D.SETTLEMENT_AMT_UN_OC"))), "billing_amount"),
                    OOQL.CreateArithmetic((OOQL.CreateProperty("PURCHASE_RECEIPT_D.AMOUNT_UNINCLUDE_TAX_OC")
                        + OOQL.CreateProperty("PURCHASE_RECEIPT_D.TAX_OC")),
                                Formulas.Case(null, OOQL.CreateProperty("PURCHASE_RECEIPT_D.SETTLEMENT_AMT_UN_OC")
                                + OOQL.CreateProperty("PURCHASE_RECEIPT_D.SETTLEMENT_TAX_OC"),
                                        OOQL.CreateCaseArray(
                                                OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_ARRIVAL.TAX_INCLUDED") == OOQL.CreateConstants(true, GeneralDBType.Boolean)),
                                                        OOQL.CreateProperty("PURCHASE_RECEIPT_D.SETTLEMENT_AMT_UN_OC")))), ArithmeticOperators.Sub, "not_billing_amount"))
                    //20170508 add by liwei1 for P001-161209002 ---end---
                    .From("PURCHASE_RECEIPT", "PURCHASE_RECEIPT")
                    .InnerJoin("PURCHASE_RECEIPT.PURCHASE_RECEIPT_D", "PURCHASE_RECEIPT_D")
                    .On((OOQL.CreateProperty("PURCHASE_RECEIPT_D.PURCHASE_RECEIPT_ID") ==OOQL.CreateProperty("PURCHASE_RECEIPT.PURCHASE_RECEIPT_ID")))
                    #region //20170508 mark by liwei1 for P001-161209002
                    //.InnerJoin(
                    //    OOQL.Select(
                    //        OOQL.CreateProperty("PAYABLE_DOC_D.SOURCE2_ID.ROid", "SOURCE2_ID_ROid"),
                    //        OOQL.CreateProperty("PAYABLE_DOC_D.DISCOUNTED_PRICE_INTAX", "DISCOUNTED_PRICE_INTAX"),
                    //        Formulas.Sum(
                    //            OOQL.CreateProperty("PAYABLE_DOC_D.PRICE_QTY"), "PRICE_QTY"),
                    //        Formulas.Sum(
                    //            OOQL.CreateProperty("PAYABLE_DOC_D.AMT_TC"), "AMT_TC"))
                    //        .From("PAYABLE_DOC.PAYABLE_DOC_D", "PAYABLE_DOC_D")
                    //        .InnerJoin("PAYABLE_DOC", "PAYABLE_DOC")
                    //        .On((OOQL.CreateProperty("PAYABLE_DOC.PAYABLE_DOC_ID") ==OOQL.CreateProperty("PAYABLE_DOC_D.PAYABLE_DOC_ID")))
                    //        .Where((OOQL.CreateProperty("PAYABLE_DOC.ApproveStatus") ==OOQL.CreateConstants("Y"))
                    //               &((OOQL.CreateProperty("PAYABLE_DOC.BOOKKEEPING_DATE") >=OOQL.CreateConstants(dateS.ToDate()))
                    //               &(OOQL.CreateProperty("PAYABLE_DOC.DOC_DATE") <=OOQL.CreateConstants(dateE.ToDate()))))
                    //        .GroupBy(
                    //            OOQL.CreateProperty("PAYABLE_DOC_D.SOURCE2_ID.ROid"),
                    //            OOQL.CreateProperty("PAYABLE_DOC_D.DISCOUNTED_PRICE_INTAX")), "PAYABLE_DOC_D")
                    //.On((OOQL.CreateProperty("PAYABLE_DOC_D.SOURCE2_ID_ROid") ==OOQL.CreateProperty("PURCHASE_RECEIPT_D.PURCHASE_RECEIPT_D_ID")))
                    #endregion
                    //20170508 add by liwei1 for P001-161209002 ---gebin---
                    .InnerJoin("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                    .On((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.SOURCE_ID")))
                    .InnerJoin("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                    .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID")))
                    .LeftJoin("UNIT", "B_UNIT")
                    .On((OOQL.CreateProperty("B_UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.BUSINESS_UNIT_ID")))
                    //20170508 add by liwei1 for P001-161209002 ---end---
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID") ==OOQL.CreateProperty("PURCHASE_RECEIPT_D.ORDER_SOURCE_ID.ROid")))
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") ==OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID")))
                    .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") ==OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                    .InnerJoin("PLANT", "PLANT")
                    .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT.Owner_Org.ROid")))
                    .InnerJoin("ITEM", "ITEM")
                    .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.ITEM_ID")))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")//20171021 add by liwei1 for B001-171020001
                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.ITEM_FEATURE_ID")))//20171021 add by liwei1 for B001-171020001
                    .LeftJoin("UNIT", "UNIT")
                    .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.PRICE_UNIT_ID")))
                    .InnerJoin("SUPPLIER", "SUPPLIER")
                    .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") ==OOQL.CreateProperty("PURCHASE_RECEIPT.SUPPLIER_ID")));

            #region 20171010 add by zhangcn for B001-171010004
            QueryConditionGroup conditionGroup2 =
                (OOQL.AuthFilter("PURCHASE_ISSUE", "PURCHASE_ISSUE"))
                & (OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateConstants(supplierNo))
                & (OOQL.CreateProperty("PURCHASE_ISSUE.TRANSACTION_DATE") >= OOQL.CreateConstants(dateS.ToDate()))
                & (OOQL.CreateProperty("PURCHASE_ISSUE.TRANSACTION_DATE") <= OOQL.CreateConstants(dateE.ToDate()))
                & (OOQL.CreateProperty("PURCHASE_ISSUE.ApproveStatus") == OOQL.CreateConstants("Y"));
            
            //如果营运中心不为空增加条件
            if (!Maths.IsEmpty(siteNo)) {
                conditionGroup2 &= (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo));
            }        

            QueryNode unioNode =
                OOQL.Select(OOQL.CreateProperty("PURCHASE_ISSUE.TRANSACTION_DATE", "create_date"),
                            OOQL.CreateProperty("PURCHASE_ISSUE.DOC_NO", "stock_in_no"),
                            OOQL.CreateProperty("PURCHASE_ISSUE_D.SequenceNumber", "seq"),
                            OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                            OOQL.CreateProperty("PURCHASE_ISSUE_D.ITEM_DESCRIPTION", "item_name"),
                            OOQL.CreateProperty("PURCHASE_ISSUE_D.ITEM_SPECIFICATION", "item_spec"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty), "item_feature_no"),//20171021 add by liwei1 for B001-171020001
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_feature_name"),//20171021 add by liwei1 for B001-171020001
                            //OOQL.CreateProperty("PURCHASE_ISSUE_D.BUSINESS_QTY", "qty"),
                            OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ISSUE_D.BUSINESS_QTY"), 
                                                  OOQL.CreateConstants(-1),
                                                  ArithmeticOperators.Mulit,
                                                  "qty"),
                            Formulas.IsNull(OOQL.CreateProperty("B_UNIT.UNIT_CODE"),OOQL.CreateConstants(string.Empty, GeneralDBType.String), "unit_no"),
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"),OOQL.CreateConstants(string.Empty, GeneralDBType.String), "valuation_unit_no"),
                            //OOQL.CreateProperty("PURCHASE_ISSUE_D.PRICE_QTY", "valuation_qty"),
                            OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ISSUE_D.PRICE_QTY"),
                                                  OOQL.CreateConstants(-1),
                                                  ArithmeticOperators.Mulit,
                                                  "valuation_qty"),
                            OOQL.CreateProperty("PURCHASE_RETURN_D.DISCOUNTED_PRICE", "price"),
                            //OOQL.CreateProperty("PURCHASE_ISSUE_D.AMOUNT_UNINCLUDE_TAX_OC", "amount"), 
                            OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ISSUE_D.AMOUNT_UNINCLUDE_TAX_OC"),
                                                  OOQL.CreateConstants(-1),
                                                  ArithmeticOperators.Mulit,
                                                  "amount"),
                            //OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ISSUE_D.AMOUNT_UNINCLUDE_TAX_OC"),
                            //                      OOQL.CreateProperty("PURCHASE_ISSUE_D.TAX_OC"), 
                            //                      ArithmeticOperators.Plus, 
                            //                      "tax_amount"),
                            OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ISSUE_D.AMOUNT_UNINCLUDE_TAX_OC") + OOQL.CreateProperty("PURCHASE_ISSUE_D.TAX_OC"),
                                                  OOQL.CreateConstants(-1),
                                                  ArithmeticOperators.Mulit,
                                                  "tax_amount"),
                            OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "purchase_no"),
                            OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber", "purchase_seq"),
                            //OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_PRICE_QTY","billing_qty"),
                            OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_PRICE_QTY"),
                                                  OOQL.CreateConstants(-1),
                                                  ArithmeticOperators.Mulit,
                                                  "billing_qty"),
                            //OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ISSUE_D.PRICE_QTY"),
                            //                      OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_PRICE_QTY"),
                            //                      ArithmeticOperators.Sub,
                            //                      "not_billing_qty"),
                            OOQL.CreateArithmetic(OOQL.CreateProperty("PURCHASE_ISSUE_D.PRICE_QTY") - OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_PRICE_QTY"),
                                                  OOQL.CreateConstants(-1),
                                                  ArithmeticOperators.Mulit,
                                                  "not_billing_qty"),
                            //Formulas.Case(null,
                            //              OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_AMOUNT_OC") + OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_TAX_OC"),
                            //              OOQL.CreateCaseArray(OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_RETURN.TAX_INCLUDED") == OOQL.CreateConstants(true, GeneralDBType.Boolean)),
                            //                                   OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_AMOUNT_OC"))), 
                            //              "billing_amount"),
                            OOQL.CreateArithmetic((Formulas.Case(null,
                                                                OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_AMOUNT_OC") + OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_TAX_OC"),
                                                                OOQL.CreateCaseArray(OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_RETURN.TAX_INCLUDED") == OOQL.CreateConstants(true, GeneralDBType.Boolean)),
                                                                                     OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_AMOUNT_OC")))
                                                               )  
                                                  ),
                                                 OOQL.CreateConstants(-1),
                                                 ArithmeticOperators.Mulit,
                                                 "billing_amount"),
                            //OOQL.CreateArithmetic((OOQL.CreateProperty("PURCHASE_ISSUE_D.AMOUNT_UNINCLUDE_TAX_OC") + OOQL.CreateProperty("PURCHASE_ISSUE_D.TAX_OC")),
                            //                       Formulas.Case(null,
                            //                                     OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_AMOUNT_OC") + OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_TAX_OC"),
                            //                                     OOQL.CreateCaseArray(OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_RETURN.TAX_INCLUDED") == OOQL.CreateConstants(true, GeneralDBType.Boolean)),
                            //                                                          OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_AMOUNT_OC")))), 
                            //                      ArithmeticOperators.Sub, 
                            //                      "not_billing_amount"))
                           OOQL.CreateArithmetic((OOQL.CreateArithmetic((OOQL.CreateProperty("PURCHASE_ISSUE_D.AMOUNT_UNINCLUDE_TAX_OC") + OOQL.CreateProperty("PURCHASE_ISSUE_D.TAX_OC")),
                                                                         Formulas.Case(null,
                                                                                       OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_AMOUNT_OC") + OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_TAX_OC"),
                                                                                       OOQL.CreateCaseArray(OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_RETURN.TAX_INCLUDED") == OOQL.CreateConstants(true, GeneralDBType.Boolean)),
                                                                                                            OOQL.CreateProperty("PURCHASE_ISSUE_D.SETTLEMENT_AMOUNT_OC")))), 
                                                                          ArithmeticOperators.Sub)), 
                                                  OOQL.CreateConstants(-1),
                                                  ArithmeticOperators.Mulit,
                                                  "not_billing_amount"))
                    .From("PURCHASE_ISSUE", "PURCHASE_ISSUE")
                    .InnerJoin("PURCHASE_ISSUE.PURCHASE_ISSUE_D", "PURCHASE_ISSUE_D")
                    .On(OOQL.CreateProperty("PURCHASE_ISSUE.PURCHASE_ISSUE_ID") == OOQL.CreateProperty("PURCHASE_ISSUE_D.PURCHASE_ISSUE_ID"))
                    .InnerJoin("PURCHASE_RETURN.PURCHASE_RETURN_D", "PURCHASE_RETURN_D")
                    .On(OOQL.CreateProperty("PURCHASE_ISSUE_D.SOURCE_ID.ROid") == OOQL.CreateProperty("PURCHASE_RETURN_D.PURCHASE_RETURN_D_ID"))
                    .InnerJoin("PURCHASE_RETURN", "PURCHASE_RETURN")
                    .On(OOQL.CreateProperty("PURCHASE_RETURN.PURCHASE_RETURN_ID") == OOQL.CreateProperty("PURCHASE_RETURN_D.PURCHASE_RETURN_ID"))
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                    .On(OOQL.CreateProperty("PURCHASE_ISSUE_D.ORDER_SOURCE_ID.ROid") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID"))
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID"))
                    .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_ISSUE.Owner_Org.ROid"))
                    .InnerJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ISSUE_D.ITEM_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")//20171021 add by liwei1 for B001-171020001
                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("PURCHASE_ISSUE_D.ITEM_FEATURE_ID")))//20171021 add by liwei1 for B001-171020001
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ISSUE_D.PRICE_UNIT_ID"))
                    .LeftJoin("UNIT", "B_UNIT")
                    .On(OOQL.CreateProperty("B_UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ISSUE_D.BUSINESS_UNIT_ID"))
                    .InnerJoin("SUPPLIER", "SUPPLIER")
                    .On(OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ISSUE.SUPPLIER_ID"))
                    .Where(conditionGroup2) ;

            #endregion

            //初始Where条件
            QueryConditionGroup conditionGroup = (OOQL.AuthFilter("PURCHASE_RECEIPT", "PURCHASE_RECEIPT"))
                                                 & ((OOQL.CreateProperty("PURCHASE_RECEIPT.ApproveStatus") ==OOQL.CreateConstants("Y"))
                                                 & (OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") ==OOQL.CreateConstants(supplierNo))
                                                 & (OOQL.CreateProperty("PURCHASE_RECEIPT.TRANSACTION_DATE") >= OOQL.CreateConstants(dateS.ToDate()))//20170508 add by liwei1 for P001-161209002 
                                                 & (OOQL.CreateProperty("PURCHASE_RECEIPT.TRANSACTION_DATE") <= OOQL.CreateConstants(dateE.ToDate()))//20170508 add by liwei1 for P001-161209002 
                                                 );

            //如果营运中心不为空增加条件
            if (!Maths.IsEmpty(siteNo)) {
                conditionGroup &= (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo));
            }
            return joinOnNode.Where(conditionGroup).Union(unioNode);//20171010 modi by zhangcn for B001-171010004 【增加.Union(unioNode)】
        }
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-13</createDate>
//<description>退换货清单查询服务</description>
//---------------------------------------------------------------- 
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
    /// 退换货清单查询服务
    /// </summary>
    [ServiceClass(typeof(IReturnQtyService))]
    [Description("退换货清单查询服务")]
    public class ReturnQtyService:ServiceComponent,IReturnQtyService {
        /// <summary>
        /// 根据传入的供应商查询出退换货清单
        /// </summary>
        /// <param name="supplier_no">供应商编号(必填)</param>
        /// <param name="item_no">料件编号</param>
        /// <param name="item_name">品名</param>
        /// <param name="item_spec">规格</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <returns></returns>
        public Hashtable ReturnQty(string supplier_no, string item_no, string item_name, string item_spec, string enterprise_no, string site_no) {
            try {
                if (Maths.IsEmpty(supplier_no)) {
                    IInfoEncodeContainer infoEnCode = GetService<IInfoEncodeContainer>();
                    throw new BusinessRuleException(infoEnCode.GetMessage("A111201", "supplier_no"));//‘入参【supplier_no】未传值’( A111201)
                }
                //查询出退换货清单
                QueryNode queryNode = GetReturnChangeDetail(supplier_no, item_no, item_name, item_spec, site_no);
                DependencyObjectCollection returnChangeDetail = GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //组合返回值
                Hashtable result = new Hashtable();
                result.Add("return_change_detail", returnChangeDetail);
                return result;
            } catch (Exception) {
                throw;
            } 
        }

        /// <summary>
        /// 根据传入的供应商查询出退换货清单
        /// </summary>
        /// <param name="supplierNo">供应商编号</param>
        /// <param name="itemNo">料件编号</param>
        /// <param name="itemName">品名</param>
        /// <param name="itemSpec">规格</param>
        /// <param name="siteNo">营运中心</param>
        /// <returns></returns>
        private QueryNode GetReturnChangeDetail(string supplierNo, string itemNo, string itemName, string itemSpec, string siteNo){
            JoinOnNode joinOnNode =
                OOQL.Select(
                    OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "purchase_no"),
                    OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber", "purchase_seq"),
                    Formulas.IsNull(
                        OOQL.CreateProperty("ITEM.ITEM_CODE"),
                        OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_no"),
                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_DESCRIPTION", "item_name"),
                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_SPECIFICATION", "item_spec"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_no"),   //20171010 add by zhangcn for B001-171010004
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_name"),//20171010 add by zhangcn for B001-171010004
                    Formulas.IsNull(
                        OOQL.CreateProperty("UNIT.UNIT_CODE"),
                        OOQL.CreateConstants(string.Empty, GeneralDBType.String), "unit_no"),
                    OOQL.CreateProperty("B.QTY", "qty"))
                    .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")))
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")))
                    .InnerJoin("PLANT", "PLANT")
                    .On((OOQL.CreateProperty("PLANT.PLANT_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                    .InnerJoin("ITEM", "ITEM")
                    .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")))
                    .LeftJoin("UNIT", "UNIT")
                    .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID")))
                    .InnerJoin(
                        OOQL.Select(
                            Formulas.Sum(Formulas.IsNull(
                                OOQL.CreateProperty("PURCHASE_ARRIVAL_D.RETURN_BUSINESS_QTY"),
                                OOQL.CreateConstants(0, GeneralDBType.Int32)) + Formulas.IsNull(
                                    OOQL.CreateProperty("PURCHASE_ISSUE_D.BUSINESS_QTY"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32)), "QTY"),
                            OOQL.CreateProperty("A.PURCHASE_ORDER_SD_ID"))
                            .From("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "A")
                            .LeftJoin(
                                OOQL.Select(
                                    OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SOURCE_ID.ROid", "SOURCE_ID"),
                                    OOQL.CreateProperty("PURCHASE_ARRIVAL_D.RETURN_BUSINESS_QTY"))
                                    .From("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                                    .InnerJoin("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                                    .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID") ==
                                         OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID")))
                                    .Where((OOQL.CreateProperty("PURCHASE_ARRIVAL.ApproveStatus") ==
                                            OOQL.CreateConstants("Y"))), "PURCHASE_ARRIVAL_D")
                            .On((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SOURCE_ID") ==
                                 OOQL.CreateProperty("A.PURCHASE_ORDER_SD_ID")))
                            .LeftJoin(
                                OOQL.Select(
                                    OOQL.CreateProperty("PURCHASE_ISSUE_D.ORDER_SOURCE_ID.ROid", "ORDER_SOURCE_ID"),
                                    OOQL.CreateProperty("PURCHASE_ISSUE_D.BUSINESS_QTY"))
                                    .From("PURCHASE_ISSUE.PURCHASE_ISSUE_D", "PURCHASE_ISSUE_D")
                                    .InnerJoin("PURCHASE_ISSUE", "PURCHASE_ISSUE")
                                    .On((OOQL.CreateProperty("PURCHASE_ISSUE.PURCHASE_ISSUE_ID") ==
                                         OOQL.CreateProperty("PURCHASE_ISSUE_D.PURCHASE_ISSUE_ID")))
                                    .Where((OOQL.CreateProperty("PURCHASE_ISSUE.ApproveStatus") ==
                                            OOQL.CreateConstants("Y"))), "PURCHASE_ISSUE_D")
                            .On((OOQL.CreateProperty("PURCHASE_ISSUE_D.ORDER_SOURCE_ID") ==
                                 OOQL.CreateProperty("A.PURCHASE_ORDER_SD_ID")))
                            .GroupBy(
                                OOQL.CreateProperty("A.PURCHASE_ORDER_SD_ID")), "B")
                    .On((OOQL.CreateProperty("B.PURCHASE_ORDER_SD_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID")))
                    .InnerJoin("SUPPLIER", "SUPPLIER")
                    .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID")))
                //20171010 add by zhangcn for B001-171010004  ========================begin=========================
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID") ==
                        OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"));
               //20171010 add by zhangcn for B001-171010004  ========================end=========================
            //初始Where条件
            QueryConditionGroup conditionGroup = (OOQL.AuthFilter("PURCHASE_ORDER", "PURCHASE_ORDER"))
                                                & ((OOQL.CreateProperty("PURCHASE_ORDER.ApproveStatus") == OOQL.CreateConstants("Y"))
                                                & (OOQL.CreateProperty("PURCHASE_ORDER.CLOSE") == OOQL.CreateConstants("0"))
                                                & (OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateConstants(supplierNo)))
                                                & (OOQL.CreateProperty("B.QTY") > OOQL.CreateConstants(0m));
            //如果【品号】不为空增加条件
            if (!Maths.IsEmpty(itemNo)){
                conditionGroup &= (OOQL.CreateProperty("ITEM.ITEM_CODE").Like(OOQL.CreateConstants("%" + itemNo + "%")));
            }
            //如果【品名】不为空增加条件
            if (!Maths.IsEmpty(itemName)) {
                conditionGroup &= (OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_DESCRIPTION") .Like(OOQL.CreateConstants("%" + itemName + "%")));
            }
            //如果【规格】不为空增加条件
            if (!Maths.IsEmpty(itemSpec)) {
                conditionGroup &= (OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_SPECIFICATION").Like(OOQL.CreateConstants("%" + itemSpec + "%")));
            }
            //如果【工厂】不为空增加条件
            if (!Maths.IsEmpty(siteNo)) {
                conditionGroup &= (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo));
            }
            //返回组合Node
            return joinOnNode.Where(conditionGroup);
        }
    }
}

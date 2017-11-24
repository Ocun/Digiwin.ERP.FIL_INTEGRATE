//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-04</createDate>
//<description>获取到货单服务 实现</description>
//----------------------------------------------------------------
//20161226 modi by liwei1 for P001-161215001 逻辑调整
//20170117 modi by liwei1 for B001-170117019
//20170302 modi by shenbao for P001-170302002 误差率统一乘100
//20170424 modi by wangyq for P001-170420001
//20170903 modi by liwei1 for B001-170904001

// add by 08628 for P001-171023001 20171101
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取到货单服务
    /// </summary>
    [ServiceClass(typeof(IGetPurchaseArrivalService))]
    [Description("获取到货单服务")]
    public class GetPurchaseArrivalService : ServiceComponent, IGetPurchaseArrivalService {
        #region 接口方法
        /// <summary>
        /// 查询到货单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetPurchaseArrival(string programJobNo, string scanType, string status, string[] docNo, string id, string siteNo) {//20161216 add by liwei1 for P001-161215001
            //public DependencyObjectCollection GetPurchaseArrival(string programJobNo, string scanType, string status, string docNo, string id, string siteNo) {//20161216 mark by liwei1 for P001-161215001

            #region 参数检查
            if (Maths.IsEmpty(status)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));//‘入参【status】未传值’
            }
            if (Maths.IsEmpty(docNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "doc_no"));//‘入参【doc_no】未传值’
            }
            if (Maths.IsEmpty(siteNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));//‘入参【site_no】未传值’
            }
            #endregion

            //查询到货单信息
            QueryNode queryNode = GetPurchaseArrivalQueryNode(programJobNo, id, siteNo, scanType, docNo, status);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 查询到货单查询信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="id">主键</param>
        /// <param name="siteNo">工厂编号</param>   
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="status"></param>
        /// <returns></returns>
        private QueryNode GetPurchaseArrivalQueryNode(string programJobNo, string id, string siteNo, string scanType, string[] docNo, string status) {//20161216 add by liwei1 for P001-161215001
            //private QueryNode GetPurchaseArrivalQueryNode(string programJobNo, string id, string siteNo, string scanType, string docNo, string status) {//20161216 mark by liwei1 for P001-161215001

            QueryConditionGroup conditionGroup = (OOQL.AuthFilter("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")) &
                                                 ((OOQL.CreateProperty("PURCHASE_ARRIVAL.CATEGORY") == OOQL.CreateConstants("36")) &
                                                 (OOQL.CreateProperty("PURCHASE_ARRIVAL.ApproveStatus") == OOQL.CreateConstants("Y")) &
                                                 (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.RECEIPT_CLOSE") == OOQL.CreateConstants("0")) &
                                                 (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)));
            if (scanType == "1") {
                //箱条码
                conditionGroup &= (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID") ==
                                   Formulas.Cast(OOQL.CreateConstants(id), GeneralDBType.Guid));
            } else if (scanType == "2") {
                //单据条码
                conditionGroup &= (OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo)));//20161216 add by liwei1 for P001-161215001
                //conditionGroup &= (OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO") == OOQL.CreateConstants(docNo));//20161216 mark by liwei1 for P001-161215001
            }

            string docType = programJobNo + status;

            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO", "source_no"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_DATE", "create_date"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUSINESS_QTY", "doc_qty"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.RECEIPTED_BUSINESS_QTY", "in_out_qty"),
                            Formulas.Cast(OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SequenceNumber"), GeneralDBType.Decimal, "seq"),
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                            OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE", "main_organization"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_CODE"), OOQL.CreateConstants(string.Empty), "item_no"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty), "item_feature_no"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_feature_name"),
                            Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(string.Empty), "warehouse_no"),
                            Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty), "storage_spaces_no"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(string.Empty), "lot_no"),
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "unit_no"),
                            OOQL.CreateConstants("99", "enterprise_no"),
                            OOQL.CreateConstants(programJobNo, "source_operation"),
                            OOQL.CreateConstants(docType, "doc_type"),
                            OOQL.CreateConstants(0m, GeneralDBType.Decimal, "doc_line_seq"),
                            OOQL.CreateConstants(0m, GeneralDBType.Decimal, "doc_batch_seq"),
                            //OOQL.CreateConstants(string.Empty, "object_no")//20170903 mark by liwei1 for B001-170904001
                            OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE", "object_no")//20170903 add by liwei1 for B001-170904001
                //20161226 add by liwei1 for P001-161215001 ===begin===
                            , Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty), "item_name"),  //品名
                            Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_spec"),  //规格
                            Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                                new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL")==OOQL.CreateConstants("N")
                                    ,OOQL.CreateConstants("2"))
                            }), OOQL.CreateConstants(string.Empty), "lot_control_type"),  //批号管控方式
                            OOQL.CreateArithmetic(Formulas.IsNull(OOQL.CreateProperty("ITEM_PURCHASE.RECEIPT_OVER_RATE"), OOQL.CreateConstants(0))
                                , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"),  //允许误差率//20170302 modi by shenbao for P001-170302002 误差率统一乘100
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]{ OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_ID")
                                , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                , OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUSINESS_UNIT_ID")
                                , OOQL.CreateConstants(1)}),  //单位转换率分母
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]{ OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_ID")
                                , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                , OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUSINESS_UNIT_ID")
                                , OOQL.CreateConstants(0)}),  //单位转换率分子
                            Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "inventory_unit"),  //库存单位
                //20161226 add by liwei1 for P001-161215001 ===end===
                            OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),//20170424 add by wangyq for P001-170420001
                            OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"),//20170424 add by wangyq for P001-170420001
                // add by 08628 for P001-171023001 b
                    OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_CODE", "main_warehouse_no"),
                     OOQL.CreateProperty("MAIN_BIN.BIN_CODE", "main_storage_no"),
                   OOQL.CreateConstants(string.Empty, "first_in_first_out_control")
                // add by 08628 for P001-171023001 e
                            )
                     .From("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                     .InnerJoin("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                     .On(OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID"))
                     .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                     .On(OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.ROid"))
                     .InnerJoin("PLANT", "PLANT")
                     .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.RECEIVE_Owner_Org.ROid"))
                     .InnerJoin("ITEM", "ITEM")
                     .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_ID"))
                     .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                     .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_FEATURE_ID"))
                     .LeftJoin("WAREHOUSE", "WAREHOUSE")
                     .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.WAREHOUSE_ID"))
                     .LeftJoin("WAREHOUSE.BIN", "BIN")
                     .On(OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BIN_ID"))
                     .LeftJoin("ITEM_LOT", "ITEM_LOT")
                     .On(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_LOT_ID"))
                     .LeftJoin("UNIT", "UNIT")
                     .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUSINESS_UNIT_ID"))
                //20161226 add by liwei1 for P001-161215001 ===begin===
                    .LeftJoin("ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                        & OOQL.CreateProperty("PURCHASE_ARRIVAL.RECEIVE_Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))//20170117 add by liwei1 for B001-170117019
                //& OOQL.CreateProperty("PURCHASE_ARRIVAL.PLANT_ID") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))//20170117 mark by liwei1 for B001-170117019
                    .LeftJoin("ITEM_PURCHASE", "ITEM_PURCHASE")
                    .On(OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")
                        & OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid"))
                    .LeftJoin("UNIT", "STOCK_UNIT")
                    .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                //20161226 add by liwei1 for  P001-161215001 ===end===
                    .LeftJoin("SUPPLIER", "SUPPLIER")//20170903 add by liwei1 for B001-170904001
                    .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.SUPPLIER_ID")))//20170903 add by liwei1 for B001-170904001
                // add by 08628 for P001-171023001 b
                    .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                    .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                    .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                        & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                // add by 08628 for P001-171023001 e
                    .Where(conditionGroup);

            return queryNode;
        }

        #endregion
    }
}

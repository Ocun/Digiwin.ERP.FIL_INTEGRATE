//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-3-27</createDate>
//<IssueNo>P001-170316001</IssueNo>
//<description>依条码获取送货单明细服务实现</description>
//----------------------------------------------------------------
//20170331 modi by wangrm for P001-170327001
//20170424 modi by wangyq for P001-170420001
// modi by 08628 for P001-171023001 20171101

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetFILPurchaseArrivalBcodeService))]
    [Description("依条码获取送货单明细服务 实现")]
    public class GetFILPurchaseArrivalBcodeService : ServiceComponent, IGetFILPurchaseArrivalBcodeService {
        #region IGetFILPurchaseArrivalBcodeService 成员
        /// <summary>
        /// 依条码获取送货单明细服务
        /// </summary>
        /// <param name="programJobNo">作业编号  1-1采购收货(采购单号),3-1收货入库（采购单号）</param>
        /// <param name="scanType">扫描类型1.单据条码 2.组合条码</param>
        /// <param name="status">执行动作A.新增  S.过帐</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetFILPurchaseArrivalBcode(string programJobNo, string scanType, string status, string[] docNo, string siteNo) {
            #region 参数检查
            if (Maths.IsEmpty(programJobNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "program_job_no"));//‘入参【program_job_no】未传值’ 
            }
            if (Maths.IsEmpty(status)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));//‘入参【status】未传值’
            }
            if (Maths.IsEmpty(siteNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));//‘入参【site_no】未传值’
            }
            #endregion

            //查询送货单明细
            QueryNode queryNode = GetFILPurchaseArrivalBcodeNode(programJobNo,status, docNo);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);

        }

        #endregion

        #region 业务方法
        private QueryNode GetFILPurchaseArrivalBcodeNode(string programJobNo,string status,string[] docNo) {
            QueryNode node = OOQL.Select(true,
                                         OOQL.CreateConstants("99", "enterprise_no"),
                                         OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                                         OOQL.CreateConstants(programJobNo,"source_operation"),
                                         OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO", "source_no"),//20170331 modi by wangrm for P001-170327001 OLD:PURCHASE_ORDER.DOC_NO
                                         OOQL.CreateConstants(programJobNo+status,"doc_type"),
                                         OOQL.CreateProperty("FIL_ARRIVAL.DOC_DATE", "create_date"),//20170331 modi by wangrm for P001-170327001 OLD:PURCHASE_ORDER.DOC_DATE
                                         OOQL.CreateProperty("FIL_ARRIVAL_D.SequenceNumber", "seq"),//20170331 modi by wangrm for P001-170327001 OLD:PURCHASE_ORDER_D.SequenceNumber
                                         OOQL.CreateConstants(0, "doc_line_seq"),//20170331 modi by wangrm for P001-170327001 OLD:PURCHASE_ORDER_SD.SequenceNumber
                                         OOQL.CreateConstants(0, "doc_batch_seq"),//单据分批序
                                         OOQL.CreateConstants(string.Empty,"object_no"),//对象编号
                                         OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                         OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE", "item_feature_no"),
                                         OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "item_feature_name"),
                                         OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE", "warehouse_no"),
                                         OOQL.CreateConstants(string.Empty,"storage_spaces_no"),
                                         OOQL.CreateConstants(string.Empty, "lot_no"),
                                         OOQL.CreateProperty("PURCHASE_ORDER_SD.BUSINESS_QTY", "doc_qty"),
                                         OOQL.CreateProperty("PURCHASE_ORDER_SD.ARRIVED_BUSINESS_QTY", "in_out_qty"),
                                         //20170331 add by wangrm for P001-170327001=======start==========
                                         OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "upper_no"),
                                         OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber", "upper_seq"),
                                         OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber", "upper_line_seq"),
                                         OOQL.CreateConstants(0, "upper_batch_seq"),
                                         //20170331 add by wangrm for P001-170327001=======end============
                                         OOQL.CreateProperty("UNIT.UNIT_CODE", "unit_no"),
                                         OOQL.CreateArithmetic(OOQL.CreateProperty("ITEM_PURCHASE.RECEIPT_OVER_RATE"),OOQL.CreateConstants(100,GeneralDBType.Decimal),ArithmeticOperators.Mulit, "allow_error_rate"),
                                         OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                         OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                         Formulas.Case(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL"),OOQL.CreateConstants("1"),
                                                       new CaseItem[] { new CaseItem(OOQL.CreateConstants("N"), OOQL.CreateConstants("2")) }, "lot_control_type"),
                                         Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator",
                                            new object[]{
                                                       OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID"),
                                                       OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                       OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID"),
                                                       OOQL.CreateConstants(1)
                                                        }), //单位转换率分母
                                         Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular",
                                             new object[]{
                                                       OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID"),
                                                       OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                       OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID"),
                                                       OOQL.CreateConstants(0)
                                                        }), //单位转换率分子
                                         OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE", "inventory_unit"),
                                         OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE", "main_organization"),
                                         OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),//20170424 add by wangyq for P001-170420001
                                         OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"),//20170424 add by wangyq for P001-170420001
                // modi by 08628 for P001-171023001 b
                                         OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_CODE", "main_warehouse_no"),
                                         OOQL.CreateProperty("MAIN_BIN.BIN_CODE", "main_storage_no"),
                                         OOQL.CreateConstants(string.Empty, "first_in_first_out_control")
                // modi by 08628 for P001-171023001 e
                                         )
                           .From("FIL_ARRIVAL", "FIL_ARRIVAL")
                           .InnerJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                           .On(OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID") == OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID"))
                           .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                           .On(OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO") == OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"))
                           .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                           .On(OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")
                              & OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE") == OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber"))
                           .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                           .On(OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID")
                              & OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE") == OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber"))
                           .LeftJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                           .On(OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid") == OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID"))
                           .LeftJoin("PLANT", "PLANT")
                           .On(OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                           .LeftJoin("ITEM", "ITEM")
                           .On(OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                           .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                           .On(OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                           .LeftJoin("WAREHOUSE", "WAREHOUSE")
                           .On(OOQL.CreateProperty("PURCHASE_ORDER_SD.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                           .LeftJoin("UNIT", "UNIT")
                           .On(OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                           .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                           .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                              & OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                           .LeftJoin("ITEM_PURCHASE", "ITEM_PURCHASE")
                           .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID")
                              & OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid"))
                           .LeftJoin("UNIT", "STOCK_UNIT")
                           .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                // modi by 08628 for P001-171023001 b
                           .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                           .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                           .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                           .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") == OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                           & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                // modi by 08628 for P001-171023001 e
                           .Where((OOQL.AuthFilter("FIL_ARRIVAL", "FIL_ARRIVAL"))
                           & (OOQL.CreateProperty("FIL_ARRIVAL_D.STATUS") == OOQL.CreateConstants("N"))
                             & OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo)));
            return node;
        }
        #endregion
    }
}

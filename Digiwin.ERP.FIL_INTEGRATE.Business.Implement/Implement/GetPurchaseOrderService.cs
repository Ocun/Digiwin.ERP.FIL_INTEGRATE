//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-03</createDate>
//<description>获取采购订单服务 实现</description>
//----------------------------------------------------------------
//20161216 modi by liwei1 for P001-161215001 逻辑调整
//20160110 modi by shenbao for P001-170110001 修正单号多笔传入和增加允许误差率等
//20170302 modi by shenbao for P001-170302002 误差率统一乘100
//20170424 modi by wangyq for P001-170420001
//20170511 modi by liwei1 for P001-170420001
//20170619 add by zhangcn for P001-170606002 整单协同相关修改
//20170903 modi by liwei1 for B001-170904001
// add by 08628 for P001-171023001 20171101

using System.Collections.Generic;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement
{
    [ServiceClass(typeof (IGetPurchaseOrderService))]
    [Description("获取采购订单服务")]
    public class GetPurchaseOrderService : ServiceComponent, IGetPurchaseOrderService
    {
        #region 接口方法

        /// <summary>
        /// 根据传入的条码，获取相应的采购订单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="scanType">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="siteNo">单据编号</param>
        /// <param name="docNo"></param>
        /// <param name="id">ID</param>
        /// <returns></returns>
        public DependencyObjectCollection GetPurchaseOrder(string programJobNo, string status, string scanType,
            string[] docNo, string id, string siteNo)
        {
//20161216 add by liwei1 for P001-161215001
            //public DependencyObjectCollection GetPurchaseOrder(string programJobNo, string status, string scanType, string docNo, string id, string siteNo) {//20161216 mark by liwei1 for P001-161215001

            #region 参数检查

            if (Maths.IsEmpty(status))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status")); //‘入参【status】未传值’
            }
            if (Maths.IsEmpty(docNo) && Maths.IsEmpty(id))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "doc_no、ID"));
                //‘入参【doc_no、ID】未传值’//message A111201
            }
            if (Maths.IsEmpty(siteNo))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));
                //‘‘入参【site_no】未传值’//message A111201
            }

            #endregion

            //查询采购订单信息
            QueryNode queryNode = GetPurchaseOrderQueryNode(docNo, siteNo, programJobNo, scanType, id, status);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 获取采购订单QueryNode
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType"></param>
        /// <param name="id"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        private QueryNode GetPurchaseOrderQueryNode(string[] docNo, string siteNo, string programJobNo, string scanType,
            string id, string status)
        {
//20161216 add by liwei1 for P001-161215001
            //private QueryNode GetPurchaseOrderQueryNode(string docNo, string siteNo, string programJobNo,string scanType,string id,string status) {//20161216 mark by liwei1 for P001-161215001

            //where条件中存在Or条件，考虑到性能问题：OOQL写成Union形式
            // 条件：
            // （【入参scan_type】 = 1.箱条码’ 
            //AND  采购订单子单身.主键(PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID) = 【入参ID】）
            //OR(【入参scan_type】 = 2.单据条码
            //采购订单信息.单号(PURCHASE_ORDER.DOC_NO) = 【入参doc_no】 AND)

            List<QueryProperty> lstQueryProperties = new List<QueryProperty>();
            lstQueryProperties.AddRange(new QueryProperty[]
            {
                OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "source_no"),
                OOQL.CreateConstants(programJobNo + status, GeneralDBType.String, "doc_type"),
                OOQL.CreateProperty("PURCHASE_ORDER.DOC_DATE", "create_date"),
                OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber", "seq"),
                OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber", "doc_line_seq"),
                Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Int32, "doc_batch_seq"),
                //OOQL.CreateConstants(string.Empty, GeneralDBType.String, "object_no"),//20170903 mark by liwei1 for B001-170904001
                OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE", "object_no"), //20170903 add by liwei1 for B001-170904001
                OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                Formulas.IsNull(
                    OOQL.CreateProperty("ITEM.ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_no"),
                Formulas.IsNull(
                    OOQL.CreateProperty("ITEM.ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_name"),
                Formulas.IsNull(
                    OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "warehouse_no"),
                OOQL.CreateConstants(string.Empty, GeneralDBType.String, "storage_spaces_no"),
                OOQL.CreateConstants(string.Empty, GeneralDBType.String, "lot_no"),
                OOQL.CreateProperty("PURCHASE_ORDER_SD.BUSINESS_QTY", "doc_qty"),
                OOQL.CreateProperty("PURCHASE_ORDER_SD.ARRIVED_BUSINESS_QTY", "in_out_qty"),
                OOQL.CreateProperty("PURCHASE_ORDER_SD.PLAN_ARRIVAL_DATE", "in_out_date1"),
                //20170511 add by liwei1 for P001-170420001
                Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "unit_no"),
                OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE", "main_organization"),
                //20160110 add by shenbao for P001-170110001 ===begin===
                Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty), "item_name"),
                //品名
                Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty),
                    "item_spec"), //规格
                OOQL.CreateArithmetic(
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_PURCHASE.RECEIPT_OVER_RATE"), OOQL.CreateConstants(0))
                    , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"),
                //允许误差率  //20170302 modi by shenbao for P001-170302002 误差率统一乘100
                Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]
                {
                    new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N")
                        , OOQL.CreateConstants("2"))
                }), OOQL.CreateConstants(string.Empty), "lot_control_type"), //批号管控方式
                Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]
                {
                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")
                    , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                    , OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID")
                    , OOQL.CreateConstants(1)
                }), //单位转换率分母
                Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]
                {
                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")
                    , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                    , OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID")
                    , OOQL.CreateConstants(0)
                }), //单位转换率分子
                Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty),
                    "inventory_unit"), //库存单位
                //20160110 add by shenbao for P001-170110001 ===end===
                OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"), //20170424 add by wangyq for P001-170420001
                OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"),
                //20170424 add by wangyq for P001-170420001
                // add by 08628 for P001-171023001 b
                OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_CODE", "main_warehouse_no"),
                OOQL.CreateProperty("MAIN_BIN.BIN_CODE", "main_storage_no"),
              OOQL.CreateConstants(string.Empty, "first_in_first_out_control")
                // add by 08628 for P001-171023001 e
            }
                );

            #region  20170619 add by zhangcn for P001-170606002  PURCHASE_ORDER.ALL_SYNERGY = True

            QueryNode nodeAllSynery =
                OOQL.Select(lstQueryProperties)
                    .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")))
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")))
                    .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                    .On((OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid")))
                    //20170619 add by zhangcn for P001-170606002 ===begin===
                    .LeftJoin("SUPPLY_SYNERGY", "SUPPLY_SYNERGY")
                    .On(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID") ==
                        OOQL.CreateProperty("PURCHASE_ORDER.GROUP_SYNERGY_ID.ROid"))
                    .LeftJoin("PLANT", "PLANTSSY")
                    .On(OOQL.CreateProperty("PLANTSSY.PLANT_ID") ==
                        OOQL.CreateProperty("SUPPLY_SYNERGY.REQUIRE_Owner_Org.ROid"))
                    //20170619 add by zhangcn for P001-170606002 ===end===
                    .InnerJoin("PLANT", "PLANT")
                    .On((OOQL.CreateProperty("PLANT.PLANT_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                    .InnerJoin("ITEM", "ITEM")
                    .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID")))
                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER_SD.WAREHOUSE_ID")))
                    .LeftJoin("UNIT", "UNIT")
                    .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID")))
                    //20160110 add by shenbao for P001-170110001 ===begin===
                    .LeftJoin("ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                        &
                        OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid") ==
                        OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                    .LeftJoin("ITEM_PURCHASE")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID")
                        &
                        OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid") ==
                        OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid"))
                    .LeftJoin("UNIT", "STOCK_UNIT")
                    .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                    //20160110 add by shenbao for P001-170110001 ===end===
                    .LeftJoin("SUPPLIER", "SUPPLIER") //20170903 add by liwei1 for B001-170904001
                    .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") ==
                         OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID"))) //20170903 add by liwei1 for B001-170904001
                    // add by 08628 for P001-171023001 b
                    .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                    .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                    .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                        & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                    // add by 08628 for P001-171023001 e
                    .Where(OOQL.AuthFilter("PURCHASE_ORDER", "PURCHASE_ORDER")
                           &
                           (OOQL.CreateProperty("PURCHASE_ORDER.ApproveStatus") ==
                            OOQL.CreateConstants("Y", GeneralDBType.String))
                           &
                           (OOQL.CreateProperty("PLANT.PLANT_CODE") ==
                            OOQL.CreateConstants(siteNo, GeneralDBType.String))
                           & (OOQL.CreateConstants("1") == OOQL.CreateConstants(scanType, GeneralDBType.String))
                           &
                           (OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID") ==
                            OOQL.CreateConstants(id, GeneralDBType.Guid))
                           &
                           (OOQL.CreateProperty("PURCHASE_ORDER.ALL_SYNERGY") ==
                            OOQL.CreateConstants(1, GeneralDBType.Boolean)) //20170619 add by zhangcn for P001-170606002
                           &
                           (OOQL.CreateProperty("PURCHASE_ORDER.GENERATE_STATUS") ==
                            OOQL.CreateConstants(1, GeneralDBType.Boolean)) //20170619 add by zhangcn for P001-170606002
                           &
                           (OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_COMPANY_ID") ==
                            OOQL.CreateProperty("PLANTSSY.COMPANY_ID")) //20170619 add by zhangcn for P001-170606002
                           & (OOQL.CreateProperty("SUPPLY_SYNERGY.GENERATE_DIRE") == OOQL.CreateConstants("2"))
                    //20170619 add by zhangcn for P001-170606002
                    ) //20170619 add by zhangcn for P001-170606002
                    .Union(
                        OOQL.Select(lstQueryProperties)
                            .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                            .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                            .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") ==
                                 OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")))
                            .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                            .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") ==
                                 OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")))
                            .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                            .On((OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID") ==
                                 OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid")))
                            //20170619 add by zhangcn for P001-170606002 ===begin===
                            .LeftJoin("SUPPLY_SYNERGY", "SUPPLY_SYNERGY")
                            .On(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID") ==
                                OOQL.CreateProperty("PURCHASE_ORDER.GROUP_SYNERGY_ID.ROid"))
                            .LeftJoin("PLANT", "PLANTSSY")
                            .On(OOQL.CreateProperty("PLANTSSY.PLANT_ID") ==
                                OOQL.CreateProperty("SUPPLY_SYNERGY.REQUIRE_Owner_Org.ROid"))
                            //20170619 add by zhangcn for P001-170606002 ===end===
                            .InnerJoin("PLANT", "PLANT")
                            .On((OOQL.CreateProperty("PLANT.PLANT_ID") ==
                                 OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                            .InnerJoin("ITEM", "ITEM")
                            .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")))
                            .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                            .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") ==
                                 OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID")))
                            .LeftJoin("WAREHOUSE", "WAREHOUSE")
                            .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") ==
                                 OOQL.CreateProperty("PURCHASE_ORDER_SD.WAREHOUSE_ID")))
                            .LeftJoin("UNIT", "UNIT")
                            .On((OOQL.CreateProperty("UNIT.UNIT_ID") ==
                                 OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID")))
                            //20160110 add by shenbao for P001-170110001 ===begin===
                            .LeftJoin("ITEM_PLANT")
                            .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                                &
                                OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid") ==
                                OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                            .LeftJoin("ITEM_PURCHASE")
                            .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID")
                                &
                                OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid") ==
                                OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid"))
                            .LeftJoin("UNIT", "STOCK_UNIT")
                            .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                            //20160110 add by shenbao for P001-170110001 ===end===
                            .LeftJoin("SUPPLIER", "SUPPLIER") //20170903 add by liwei1 for B001-170904001
                            .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") ==
                                 OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID")))
                            //20170903 add by liwei1 for B001-170904001
                            // add by 08628 for P001-171023001 b
                            .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                            .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") ==
                                OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                            .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                            .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") ==
                                OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                                & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                            // add by 08628 for P001-171023001 e
                            .Where(OOQL.AuthFilter("PURCHASE_ORDER", "PURCHASE_ORDER")
                                   &
                                   (OOQL.CreateProperty("PURCHASE_ORDER.ApproveStatus") ==
                                    OOQL.CreateConstants("Y", GeneralDBType.String))
                                   &
                                   (OOQL.CreateProperty("PLANT.PLANT_CODE") ==
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String))
                                   & (OOQL.CreateConstants("2", GeneralDBType.String) == OOQL.CreateConstants(scanType))
                                   &
                                   (OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO")
                                       .In(OOQL.CreateDyncParameter("DOC_NO1", docNo)))
                                   &
                                   (OOQL.CreateProperty("PURCHASE_ORDER.ALL_SYNERGY") ==
                                    OOQL.CreateConstants(1, GeneralDBType.Boolean))
                                //20170619 add by zhangcn for P001-170606002
                                   &
                                   (OOQL.CreateProperty("PURCHASE_ORDER.GENERATE_STATUS") ==
                                    OOQL.CreateConstants(1, GeneralDBType.Boolean))
                                //20170619 add by zhangcn for P001-170606002
                                   &
                                   (OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_COMPANY_ID") ==
                                    OOQL.CreateProperty("PLANTSSY.COMPANY_ID"))
                                //20170619 add by zhangcn for P001-170606002
                                   & (OOQL.CreateProperty("SUPPLY_SYNERGY.GENERATE_DIRE") == OOQL.CreateConstants("2"))
                            //20170619 add by zhangcn for P001-170606002
                            )
                    ); //20161216 add by liwei1 for P001-161215001

            #endregion

            return OOQL.Select(lstQueryProperties)
                .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") ==
                     OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")))
                .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") ==
                     OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")))
                .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                .On((OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID") ==
                     OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid")))
                .InnerJoin("PLANT", "PLANT")
                .On((OOQL.CreateProperty("PLANT.PLANT_ID") ==
                     OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                .InnerJoin("ITEM", "ITEM")
                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") ==
                     OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID")))
                .LeftJoin("WAREHOUSE", "WAREHOUSE")
                .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") ==
                     OOQL.CreateProperty("PURCHASE_ORDER_SD.WAREHOUSE_ID")))
                .LeftJoin("UNIT", "UNIT")
                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID")))
                //20160110 add by shenbao for P001-170110001 ===begin===
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    &
                    OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid") ==
                    OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .LeftJoin("ITEM_PURCHASE")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID")
                    &
                    OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid") ==
                    OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid"))
                .LeftJoin("UNIT", "STOCK_UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                //20160110 add by shenbao for P001-170110001 ===end===
                .LeftJoin("SUPPLIER", "SUPPLIER") //20170903 add by liwei1 for B001-170904001
                .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID")))
                //20170903 add by liwei1 for B001-170904001
                // add by 08628 for P001-171023001 b
                .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") ==
                    OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") ==
                    OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                    & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                // add by 08628 for P001-171023001 e
                .Where(OOQL.AuthFilter("PURCHASE_ORDER", "PURCHASE_ORDER")
                       &
                       (OOQL.CreateProperty("PURCHASE_ORDER.ApproveStatus") ==
                        OOQL.CreateConstants("Y", GeneralDBType.String))
                       & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo, GeneralDBType.String))
                       & (OOQL.CreateConstants("1") == OOQL.CreateConstants(scanType, GeneralDBType.String))
                       &
                       (OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID") ==
                        OOQL.CreateConstants(id, GeneralDBType.Guid))
                       &
                       (OOQL.CreateProperty("PURCHASE_ORDER.ALL_SYNERGY") ==
                        OOQL.CreateConstants(0, GeneralDBType.Boolean)) //20170619 add by zhangcn for P001-170606002
                )
                .Union(
                    OOQL.Select(lstQueryProperties)
                        .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                        .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                        .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") ==
                             OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")))
                        .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                        .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") ==
                             OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")))
                        .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                        .On((OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID") ==
                             OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid")))
                        .InnerJoin("PLANT", "PLANT")
                        .On((OOQL.CreateProperty("PLANT.PLANT_ID") ==
                             OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                        .InnerJoin("ITEM", "ITEM")
                        .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")))
                        .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                        .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") ==
                             OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID")))
                        .LeftJoin("WAREHOUSE", "WAREHOUSE")
                        .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") ==
                             OOQL.CreateProperty("PURCHASE_ORDER_SD.WAREHOUSE_ID")))
                        .LeftJoin("UNIT", "UNIT")
                        .On((OOQL.CreateProperty("UNIT.UNIT_ID") ==
                             OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID")))
                        //20160110 add by shenbao for P001-170110001 ===begin===
                        .LeftJoin("ITEM_PLANT")
                        .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                            &
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid") ==
                            OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                        .LeftJoin("ITEM_PURCHASE")
                        .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID")
                            &
                            OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid") ==
                            OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid"))
                        .LeftJoin("UNIT", "STOCK_UNIT")
                        .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                        //20160110 add by shenbao for P001-170110001 ===end===
                        .LeftJoin("SUPPLIER", "SUPPLIER") //20170903 add by liwei1 for B001-170904001
                        .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") ==
                             OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID")))
                        //20170903 add by liwei1 for B001-170904001
                        // add by 08628 for P001-171023001 b
                        .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                        .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") ==
                            OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                        .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                        .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") ==
                            OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                            & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                        // add by 08628 for P001-171023001 e
                        .Where(OOQL.AuthFilter("PURCHASE_ORDER", "PURCHASE_ORDER")
                               &
                               (OOQL.CreateProperty("PURCHASE_ORDER.ApproveStatus") ==
                                OOQL.CreateConstants("Y", GeneralDBType.String))
                               &
                               (OOQL.CreateProperty("PLANT.PLANT_CODE") ==
                                OOQL.CreateConstants(siteNo, GeneralDBType.String))
                               & (OOQL.CreateConstants("2", GeneralDBType.String) == OOQL.CreateConstants(scanType))
                               &
                               (OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO")
                                   .In(OOQL.CreateDyncParameter("DOC_NO2", docNo)))
                               &
                               (OOQL.CreateProperty("PURCHASE_ORDER.ALL_SYNERGY") ==
                                OOQL.CreateConstants(0, GeneralDBType.Boolean))
                        //20170619 add by zhangcn for P001-170606002
                        )).Union(nodeAllSynery);
            //20170619 add by zhangcn for P001-170606002 //20161216 add by liwei1 for P001-161215001
            //& (OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateConstants(docNo, GeneralDBType.String))));//20161216 mark by liwei1 for P001-161215001
        }

        #endregion
    }
}
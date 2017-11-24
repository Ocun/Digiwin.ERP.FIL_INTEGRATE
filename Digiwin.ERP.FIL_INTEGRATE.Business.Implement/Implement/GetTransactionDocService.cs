//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-04</createDate>
//<description>获取出入单服务 实现</description>
//----------------------------------------------------------------
//20161216 modi by shenbao for P001-161215001
//20170424 modi by wangyq for P001-170420001
// modi by 08628 for P001-171023001
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取出入单通知服务
    /// </summary>
    [ServiceClass(typeof(IGetTransactionDocService))]
    [Description("获取出入单服务")]
    public class GetTransactionDocService : ServiceComponent, IGetTransactionDocService {
        #region 接口方法
        /// <summary>
        /// 查询库存交易单
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetTransactionDoc(string programJobNo, string scanType, string status, string[] docNo, string id, string siteNo) {  //20161216 modi by shenbao for P001-161215001 修改docNo类型为数组
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
            QueryNode queryNode = GetTransactionDocQueryNode(programJobNo, siteNo, docNo, status);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 查询到货单查询信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="siteNo">工厂编号</param>   
        /// <param name="docNo">单据编号</param>
        /// <param name="status"></param>
        /// <returns></returns>
        private QueryNode GetTransactionDocQueryNode(string programJobNo, string siteNo, string[] docNo, string status) {  //20161216 modi by shenbao for P001-161215001 修改docNo类型为数组
            string docType = programJobNo + status;
            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO", "source_no"),
                            OOQL.CreateProperty("TRANSACTION_DOC.DOC_DATE", "create_date"),
                            OOQL.CreateProperty("TRANSACTION_DOC_D.BUSINESS_QTY", "doc_qty"),
                            OOQL.CreateConstants(0m, GeneralDBType.Decimal, "in_out_qty"),
                            Formulas.Cast(OOQL.CreateProperty("TRANSACTION_DOC_D.SequenceNumber"), GeneralDBType.Decimal, "seq"),
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"),
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
                            OOQL.CreateConstants(string.Empty, "object_no"),
                //20161216 add by shenbao FOR P001-161215001 ===begin===
                            Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty), "item_name"),  //品名
                            Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_spec"),  //规格
                            Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                                new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL")==OOQL.CreateConstants("N")
                                    ,OOQL.CreateConstants("2"))
                            }), OOQL.CreateConstants(string.Empty), "lot_control_type"),  //批号管控方式
                            OOQL.CreateConstants(0, "allow_error_rate"),  //允许误差率
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]{ OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID")
                                , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                , OOQL.CreateProperty("TRANSACTION_DOC_D.BUSINESS_UNIT_ID")
                                , OOQL.CreateConstants(1)}),  //单位转换率分母
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]{ OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID")
                                , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                , OOQL.CreateProperty("TRANSACTION_DOC_D.BUSINESS_UNIT_ID")
                                , OOQL.CreateConstants(0)}),  //单位转换率分子
                            Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "inventory_unit"),  //库存单位
                //20161216 add by shenbao FOR P001-161215001 ===end===
                            OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),//20170424 add by wangyq for P001-170420001
                            OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"),//20170424 add by wangyq for P001-170420001
                // add by 08628 for P001-171023001 b
                    Formulas.Case(null,
                        Formulas.Case(null, Formulas.Case(null, OOQL.CreateConstants("", GeneralDBType.String),
                            OOQL.CreateCaseArray(
                                OOQL.CreateCaseItem(
                                    OOQL.CreateProperty("REG_G.FIFO_TYPE").IsNotNull(),
                                    OOQL.CreateProperty("REG_G.FIFO_TYPE")))),
                            OOQL.CreateCaseArray(
                                OOQL.CreateCaseItem(
                                    OOQL.CreateProperty("REG_I.FIFO_TYPE").IsNotNull(),
                                    OOQL.CreateProperty("REG_I.FIFO_TYPE")))),
                        OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                OOQL.CreateProperty("REG_I_F.FIFO_TYPE").IsNotNull(),
                                OOQL.CreateProperty("REG_I_F.FIFO_TYPE"))), "first_in_first_out_control"),
                    OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_CODE", "main_warehouse_no"),
                    OOQL.CreateProperty("MAIN_BIN.BIN_CODE", "main_storage_no")
                // add by 08628 for P001-171023001 e
                            )
                     .From("TRANSACTION_DOC", "TRANSACTION_DOC")
                     .InnerJoin("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                     .On(OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID"))
                     .InnerJoin("PLANT", "PLANT")
                     .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.ROid"))
                     .InnerJoin("ITEM", "ITEM")
                     .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID"))
                     .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                     .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_FEATURE_ID"))
                     .LeftJoin("WAREHOUSE", "WAREHOUSE")
                     .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.WAREHOUSE_ID"))
                     .LeftJoin("WAREHOUSE.BIN", "BIN")
                     .On(OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.BIN_ID"))
                     .LeftJoin("ITEM_LOT", "ITEM_LOT")
                     .On(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_LOT_ID"))
                     .LeftJoin("UNIT", "UNIT")
                     .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.BUSINESS_UNIT_ID"))
                //20161216 add by shenbao FOR P001-161215001 ===begin===
                     .LeftJoin("ITEM_PLANT")
                     .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                        & OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                    .LeftJoin("UNIT", "STOCK_UNIT")
                    .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                //20161216 add by shenbao FOR P001-161215001 ===end===
                // add by 08628 for P001-171023001 b
                    .LeftJoin(OOQL.Select(OOQL.CreateProperty("ITEM_ID"),
                        OOQL.CreateProperty("ITEM_FEATURE_ID"),
                        OOQL.CreateProperty("FIFO_TYPE"),
                        Formulas.RowNumber("SEQ", new OverClause(new[]
                        {
                            OOQL.CreateProperty("ITEM_ID"),
                            OOQL.CreateProperty("ITEM_FEATURE_ID")
                        }, new[]
                        {
                            OOQL.CreateOrderByItem(Formulas.Case(null, OOQL.CreateProperty("CreateDate"),
                                OOQL.CreateCaseArray(
                                    OOQL.CreateCaseItem(
                                        (OOQL.CreateProperty("MAIN") == OOQL.CreateConstants(1)),
                                        Formulas.Cast(OOQL.CreateConstants("9998-12-31", GeneralDBType.String),
                                            GeneralDBType.Date)))), SortType.Desc)
                        })))
                        .From("ITEM_BC_REG")
                        .Where(OOQL.CreateProperty("ITEM_FEATURE_ID").IsNotNull()
                               &
                               (OOQL.CreateProperty("ITEM_FEATURE_ID") !=
                                OOQL.CreateConstants(Maths.GuidDefaultValue()))),
                        "REG_I_F")
                    .On((OOQL.CreateProperty("REG_I_F.ITEM_ID") ==
                         OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_D.ITEM_ID"))
                        &
                        (OOQL.CreateProperty("REG_I_F.ITEM_FEATURE_ID") ==
                         OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_D.ITEM_FEATURE_ID"))
                        & (OOQL.CreateProperty("REG_I_F.SEQ") == OOQL.CreateConstants(1, GeneralDBType.Int32)))
                    .LeftJoin(OOQL.Select(OOQL.CreateProperty("ITEM_ID"),
                        OOQL.CreateProperty("FIFO_TYPE"),
                        Formulas.RowNumber("SEQ", new OverClause(new[]
                        {
                            OOQL.CreateProperty("ITEM_ID")
                        }
                            , new[]
                            {
                                OOQL.CreateOrderByItem(Formulas.Case(null, OOQL.CreateProperty("CreateDate"),
                                    OOQL.CreateCaseArray(
                                        OOQL.CreateCaseItem(
                                            (OOQL.CreateProperty("MAIN") == OOQL.CreateConstants(1, GeneralDBType.Int32)),
                                            Formulas.Cast(OOQL.CreateConstants("9998-12-31", GeneralDBType.String),
                                                GeneralDBType.Date)))), SortType.Desc)
                            })))
                        .From("ITEM_BC_REG"), "REG_I")
                    .On((OOQL.CreateProperty("REG_I.ITEM_ID") ==
                         OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_D.ITEM_ID"))
                        & (OOQL.CreateProperty("REG_I.SEQ") == OOQL.CreateConstants(1, GeneralDBType.Int32)))
                    .LeftJoin(OOQL.Select(OOQL.CreateProperty("FEATURE_GROUP_ID"),
                        OOQL.CreateProperty("FIFO_TYPE"),
                        Formulas.RowNumber("SEQ", new OverClause(new[]
                        {
                            OOQL.CreateProperty("FEATURE_GROUP_ID"),
                        }, new[]
                        {
                            OOQL.CreateOrderByItem(Formulas.Case(null, OOQL.CreateProperty("CreateDate"),
                                OOQL.CreateCaseArray(
                                    OOQL.CreateCaseItem(
                                        (OOQL.CreateProperty("MAIN") == OOQL.CreateConstants(1, GeneralDBType.Int32)),
                                        Formulas.Cast(OOQL.CreateConstants("9998-12-31", GeneralDBType.String),
                                            GeneralDBType.Date)))), SortType.Desc)
                        })))
                        .From("ITEM_BC_REG"), "REG_G")
                    .On((OOQL.CreateProperty("REG_G.FEATURE_GROUP_ID") == OOQL.CreateProperty("ITEM.FEATURE_GROUP_ID"))
                        & (OOQL.CreateProperty("REG_G.SEQ") == OOQL.CreateConstants(1, GeneralDBType.Int32)))
                    .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                    .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                    .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                        & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                // add by 08628 for P001-171023001 e
                     .Where((OOQL.AuthFilter("TRANSACTION_DOC", "TRANSACTION_DOC")) &
                           ((OOQL.CreateProperty("TRANSACTION_DOC.ApproveStatus") == OOQL.CreateConstants("N")) &
                            (OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO").In(OOQL.CreateDyncParameter("docNos", docNo))) &  //20161216 add by shenbao FOR P001-161215001 OOQL.CreateConstants(docNo)==>.In(OOQL.CreateDyncParameter("docnos", docNo)
                            (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo))));

            return queryNode;
        }

        #endregion
    }
}

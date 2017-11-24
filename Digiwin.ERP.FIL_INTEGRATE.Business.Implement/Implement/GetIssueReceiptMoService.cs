//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-03</createDate>
//<description>获取领退料工单服务 实现</description>
//----------------------------------------------------------------
//20161208 modi by shenbao fro P001-161208001 修改退料数量
//20161216 modi by liwei1 for P001-161215001 逻辑调整
//20161216 modi by shenbao for P001-161215001
//20170223 modi by shenbao for B001-170223012 这个企业编号为字符串，无需转换
//20170302 modi by shenbao for P001-170302002 误差率统一乘100
//20170424 modi by wangyq for P001-170420001
//20170925 modi by wangyq for P001-170717001
// modi by 08628 for P001-171023001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement
{
    /// <summary>
    /// 获取领退料工单服务
    /// </summary>
    [ServiceClass(typeof (IGetIssueReceiptMoService))]
    [Description("获取领退料工单服务")]
    public class GetIssueReceiptMoService : ServiceComponent, IGetIssueReceiptMoService
    {
        #region 接口方法

        /// <summary>
        /// 根据状态码查询工单信息或者领料出库单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetIssueReceiptMo(string programJobNo, string scanType, string status,
            string[] docNo, string id, string siteNo)
        {
//20161216 add by liwei1 for P001-161215001
            //public DependencyObjectCollection GetIssueReceiptMo(string programJobNo, string scanType, string status, string docNo, string id, string siteNo) {//20161216 mark by liwei1 for P001-161215001

            #region 参数检查

            if (Maths.IsEmpty(status))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status")); //‘入参【status】未传值’
            }
            if (Maths.IsEmpty(docNo))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "doc_no")); //‘入参【doc_no】未传值’
            }
            if (Maths.IsEmpty(siteNo))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no")); //‘入参【site_no】未传值’
            }

            #endregion

            QueryNode queryNode;
            if (status == "A")
            {
                //查询工单信息
                queryNode = GetMOQueryNode(docNo, siteNo, programJobNo, status);
            }
            else
            {
                //查询领料出库单信息
                queryNode = GetIssueReceiptQueryNode(docNo, siteNo, programJobNo, status);
            }

            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 获取工单信息查询信息
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="status"></param>
        /// <returns></returns>
        private QueryNode GetMOQueryNode(string[] docNo, string siteNo, string programJobNo, string status)
        {
//20161216 add by liwei1 for P001-161215001
            //private QueryNode GetMOQueryNode(string docNo, string siteNo, string programJobNo, string status) {//20161216 mark by liwei1 for P001-161215001

            string docType = programJobNo + status;
            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("MO.DOC_NO", "source_no"),
                    OOQL.CreateProperty("MO.DOC_DATE", "create_date"),
                    Formulas.Cast(OOQL.CreateProperty("MO_D.SequenceNumber"), GeneralDBType.Decimal, "seq"),
                    //OOQL.CreateProperty("MO_D.REQUIRED_QTY", "doc_qty"),//20161208 mark by shenbao fro P001-161208001
                    //OOQL.CreateProperty("MO_D.ISSUED_QTY", "in_out_qty"),//20161208 mark by shenbao fro P001-161208001
                    //20161208 add by shenbao fro P001-161208001 ===begin===
                    Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("MO_D.ISSUED_QTY"), new CaseItem[]
                    {
                        new CaseItem(OOQL.CreateConstants(programJobNo).Like(OOQL.CreateConstants("7%"))
                            , OOQL.CreateProperty("MO_D.REQUIRED_QTY")),
                    }), OOQL.CreateConstants(0m, GeneralDBType.Decimal), "doc_qty"), //需领用量
                    Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants(0), new CaseItem[]
                    {
                        new CaseItem(OOQL.CreateConstants(programJobNo).Like(OOQL.CreateConstants("7%"))
                            , OOQL.CreateProperty("MO_D.ISSUED_QTY")),
                    }), OOQL.CreateConstants(0m, GeneralDBType.Decimal), "in_out_qty"), //已领用量
                    //20161208 add by shenbao fro P001-161208001 ===end===
                    OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                    OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_CODE"), OOQL.CreateConstants(string.Empty), "item_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                        OOQL.CreateConstants(string.Empty), "item_feature_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                        OOQL.CreateConstants(string.Empty), "item_feature_name"),
                    Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(string.Empty),
                        "warehouse_no"),
                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty),
                        "storage_spaces_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(string.Empty),
                        "lot_no"),
                    Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "unit_no"),
                    OOQL.CreateConstants("99", "enterprise_no"),
                    //20170223 modi by shenbao for B001-170223012 这个企业编号为字符串，无需转换
                    OOQL.CreateConstants(programJobNo, "source_operation"),
                    OOQL.CreateConstants(docType, "doc_type"),
                    Formulas.Cast(OOQL.CreateConstants(0m), GeneralDBType.Decimal, "doc_line_seq"),
                    Formulas.Cast(OOQL.CreateConstants(0m), GeneralDBType.Decimal, "doc_batch_seq"),
                    OOQL.CreateConstants(string.Empty, "object_no"),
                    //20161216 add by shenbao FOR P001-161215001 ===begin===
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty),
                        "item_name"), //品名
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty),
                        "item_spec"), //规格
                    Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]
                    {
                        new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N")
                            , OOQL.CreateConstants("2"))
                    }), OOQL.CreateConstants(string.Empty), "lot_control_type"), //批号管控方式
                    OOQL.CreateArithmetic(
                        Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.ISSUE_OVERRUN_RATE"), OOQL.CreateConstants(0))
                        , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"),
                    //允许误差率  //20170302 modi by shenbao for P001-170302002 误差率统一乘100
                    Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]
                    {
                        OOQL.CreateProperty("MO_D.ITEM_ID")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateProperty("MO_D.UNIT_ID")
                        , OOQL.CreateConstants(1)
                    }), //单位转换率分母
                    Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]
                    {
                        OOQL.CreateProperty("MO_D.ITEM_ID")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateProperty("MO_D.UNIT_ID")
                        , OOQL.CreateConstants(0)
                    }), //单位转换率分子
                    Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty),
                        "inventory_unit"), //库存单位
                    //20161216 add by shenbao FOR P001-161215001 ===end===
                    OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),
                    //20170424 add by wangyq for P001-170420001
                    OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"),
                    //20170424 add by wangyq for P001-170420001
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
                    .From("MO", "MO")
                    .LeftJoin("MO.MO_D", "MO_D")
                    .On(OOQL.CreateProperty("MO_D.MO_ID") == OOQL.CreateProperty("MO.MO_ID"))
                    .LeftJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("MO.Owner_Org.ROid"))
                    .LeftJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("MO_D.ITEM_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") ==
                        OOQL.CreateProperty("MO_D.ITEM_FEATURE_ID"))
                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                    .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("MO_D.WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On(OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("MO_D.BIN_ID"))
                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                    .On(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("MO_D.ITEM_LOT_ID"))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("MO_D.UNIT_ID"))
                    //20161216 add by shenbao FOR P001-161215001 ===begin===
                    .LeftJoin("ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                        & OOQL.CreateProperty("MO.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
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
                    .On((OOQL.CreateProperty("REG_I_F.ITEM_ID") == OOQL.CreateProperty("MO.MO_D.ITEM_ID"))
                        &
                        (OOQL.CreateProperty("REG_I_F.ITEM_FEATURE_ID") ==
                         OOQL.CreateProperty("MO.MO_D.ITEM_FEATURE_ID"))
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
                    .On((OOQL.CreateProperty("REG_I.ITEM_ID") == OOQL.CreateProperty("MO.MO_D.ITEM_ID"))
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
                    .Where((OOQL.AuthFilter("MO", "MO")) &
                           ((OOQL.CreateProperty("MO.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo))) &
                            //20161216 add by liwei1 for P001-161215001
                            //((OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(docNo)) &//20161216 mark by liwei1 for P001-161215001
                            (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)) &
                            (OOQL.CreateProperty("MO.ApproveStatus") == OOQL.CreateConstants("Y"))));
            return queryNode;
        }

        /// <summary>
        /// 获取领料出库单查询信息
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private QueryNode GetIssueReceiptQueryNode(string[] docNo, string siteNo, string programJobNo, string status)
        {
//20161216 add by liwei1 for P001-161215001
            //private QueryNode GetIssueReceiptQueryNode(string docNo, string siteNo, string programJobNo, string status) {//20161216 mark by liwei1 for P001-161215001
            string docType = programJobNo + status;
            //20170925 add by wangyq for P001-170717001  =============begin===============
            QueryConditionGroup group =
                OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo)) &
                OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo);
            if (programJobNo == "7-5")
            {
                group &= OOQL.CreateProperty("ISSUE_RECEIPT.ApproveStatus") == OOQL.CreateConstants("N") &
                         OOQL.CreateProperty("ISSUE_RECEIPT_D.BC_CHECK_STATUS") == OOQL.CreateConstants("1");
            }
            else
            {
                group &= OOQL.CreateProperty("ISSUE_RECEIPT.ApproveStatus") == OOQL.CreateConstants("N");
            }
            //20170925 add by wangyq for P001-170717001  =============end===============
            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO", "source_no"),
                    OOQL.CreateProperty("ISSUE_RECEIPT.DOC_DATE", "create_date"),
                    Formulas.Cast(OOQL.CreateProperty("ISSUE_RECEIPT_D.SequenceNumber"), GeneralDBType.Decimal, "seq"),
                    OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_QTY", "doc_qty"),
                    //20161230 modi by shenbao for B001-161229011 INVENTORY_QTY=>ISSUE_RECEIPT_QTY
                    Formulas.Cast(OOQL.CreateConstants(0m), GeneralDBType.Decimal, "in_out_qty"),
                    OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                    OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_CODE"), OOQL.CreateConstants(string.Empty), "item_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                        OOQL.CreateConstants(string.Empty), "item_feature_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                        OOQL.CreateConstants(string.Empty), "item_feature_name"),
                    Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(string.Empty),
                        "warehouse_no"),
                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty),
                        "storage_spaces_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(string.Empty),
                        "lot_no"),
                    Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "unit_no"),
                    OOQL.CreateConstants("99", "enterprise_no"),
                    OOQL.CreateConstants(programJobNo, "source_operation"),
                    OOQL.CreateConstants(docType, "doc_type"),
                    Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Decimal, "doc_line_seq"),
                    Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Decimal, "doc_batch_seq"),
                    OOQL.CreateConstants(string.Empty, "object_no"),
                    //20161216 add by shenbao FOR P001-161215001 ===begin===
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty),
                        "item_name"), //品名
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty),
                        "item_spec"), //规格
                    Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]
                    {
                        new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N")
                            , OOQL.CreateConstants("2"))
                    }), OOQL.CreateConstants(string.Empty), "lot_control_type"), //批号管控方式
                    OOQL.CreateArithmetic(
                        Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.ISSUE_OVERRUN_RATE"), OOQL.CreateConstants(0))
                        , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"),
                    //允许误差率//20170302 modi by shenbao for P001-170302002 误差率统一乘100
                    Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]
                    {
                        OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateProperty("ISSUE_RECEIPT_D.UNIT_ID")
                        , OOQL.CreateConstants(1)
                    }), //单位转换率分母
                    Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]
                    {
                        OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateProperty("ISSUE_RECEIPT_D.UNIT_ID")
                        , OOQL.CreateConstants(0)
                    }), //单位转换率分子
                    Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty),
                        "inventory_unit"), //库存单位
                    //20161216 add by shenbao FOR P001-161215001 ===end===
                    OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),
                    //20170424 add by wangyq for P001-170420001
                    OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"),
                    //20170424 add by wangyq for P001-170420001
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
                    .From("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                    .LeftJoin("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID") ==
                        OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID"))
                    .LeftJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.ROid"))
                    .LeftJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") ==
                        OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_FEATURE_ID"))
                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                    .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ISSUE_RECEIPT_D.WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On(OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.BIN_ID"))
                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                    .On(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") ==
                        OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_LOT_ID"))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.UNIT_ID"))
                    //20161216 add by shenbao FOR P001-161215001 ===begin===
                    .LeftJoin("ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                        &
                        OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.ROid") ==
                        OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
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
                         OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_D.ITEM_ID"))
                        &
                        (OOQL.CreateProperty("REG_I_F.ITEM_FEATURE_ID") ==
                         OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_D.ITEM_FEATURE_ID"))
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
                         OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_D.ITEM_ID"))
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
                    .Where((OOQL.AuthFilter("ISSUE_RECEIPT", "ISSUE_RECEIPT")) &
                           //20170925 modi by wangyq for P001-170717001  =============begin===============
                           group);
            //            (OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo))) &//20161216 add by liwei1 for P001-161215001
            ////(OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO") == OOQL.CreateConstants(docNo)) &//20161216 mark by liwei1 for P001-161215001
            //           (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)) &
            //           (OOQL.CreateProperty("ISSUE_RECEIPT.ApproveStatus") == OOQL.CreateConstants("N")));
            //20170925 modi by wangyq for P001-170717001  =============end===============
            return queryNode;
        }

        #endregion
    }
}
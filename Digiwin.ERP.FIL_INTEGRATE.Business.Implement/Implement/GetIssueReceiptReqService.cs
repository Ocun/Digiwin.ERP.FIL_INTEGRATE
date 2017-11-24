//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/12/26 13:49:37</createDate>
//<IssueNo>P001-161215001</IssueNo>
//<description>获取领料申请单</description>
//20170424 modi by wangyq for P001-170420001
// modi by 08628 for P001-171023001 20171101

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;
using Digiwin.Common.Query2;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取领料申请单
    /// </summary>
    [ServiceClass(typeof(IGetIssueReceiptReqService))]
    [Description("获取领料申请单")]
    public sealed class GetIssueReceiptReqService : ServiceComponent, IGetIssueReceiptReqService {
        #region IGetIssueReceiptReqService 成员
        /// <summary>
        /// 获取领料申请单
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetIssueReceiptReq(string programJobNo, string scanType, string status, string[] docNo, string id, string siteNo) {
            #region 参数检查
            if (Maths.IsEmpty(status)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));//‘入参【status】未传值’
            }
            if (Maths.IsEmpty(docNo) || docNo.Length <= 0) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "doc_no"));//‘入参【doc_no】未传值’
            }
            if (Maths.IsEmpty(siteNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));//‘入参【site_no】未传值’
            }
            #endregion

            QueryNode queryNode = GeIssueReceiptReqNode(docNo, siteNo, programJobNo, status);

            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 自定义方法

        private QueryNode GeIssueReceiptReqNode(string[] docNo, string siteNo, string programJobNo, string status) {
            string docType = programJobNo + status;
            QueryNode queryNode =
               OOQL.Select(
                    OOQL.CreateConstants("99", "enterprise_no"),  //企业编号
                    OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),  //营运据点
                    OOQL.CreateConstants(programJobNo, "source_operation"),  //来源作业
                    OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_NO", "source_no"),  //来源单号
                    OOQL.CreateConstants(docType, "doc_type"),  //单据类型
                    OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_DATE", "create_date"),  //单据日期
                    Formulas.IsNull(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.SequenceNumber"), OOQL.CreateConstants(0), "seq"),  //单据项序
                    OOQL.CreateConstants(0, "doc_line_seq"),  //单据项序
                    OOQL.CreateConstants(0, "doc_batch_seq"),  //单据分批序
                    OOQL.CreateConstants(string.Empty, "object_no"),  //对象编号
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_CODE"), OOQL.CreateConstants(string.Empty), "item_no"),  //料件编号
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty), "item_feature_no"),  //产品特征
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_feature_name"),  //产品特征说明
                    Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(string.Empty), "warehouse_no"),  //库位
                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty), "storage_spaces_no"),  //储位
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(string.Empty), "lot_no"),  //批号
                    OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.REQUEST_QTY", "doc_qty"),  //单据数量
                    OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ISSUE_RECEIPT_QTY", "in_out_qty"),  //出入数量
                    Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "unit_no"),  //单位
                    OOQL.CreateConstants(0, "allow_error_rate"),  //允许误差率       
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty), "item_name"),  //品名
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_spec"),  //规格
                    Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                                new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL")==OOQL.CreateConstants("N")
                                    ,OOQL.CreateConstants("2"))
                            }), OOQL.CreateConstants(string.Empty), "lot_control_type"),  //批号管控方式
                    Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]{ OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ITEM_ID")
                                , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                , OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.UNIT_ID")
                                , OOQL.CreateConstants(1)}),  //单位转换率分母
                    Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]{ OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ITEM_ID")
                                , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                , OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.UNIT_ID")
                                , OOQL.CreateConstants(0)}),  //单位转换率分子
                    Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "inventory_unit"),  //库存单位
                    OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"),  //主营组织 
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
                    OOQL.CreateConstants(string.Empty, "main_warehouse_no"),
                    OOQL.CreateConstants(string.Empty, "main_storage_no")
                // add by 08628 for P001-171023001 e
                    )
                    .From("ISSUE_RECEIPT_REQ", "ISSUE_RECEIPT_REQ")
                    .LeftJoin("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_D", "ISSUE_RECEIPT_REQ_D")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ISSUE_RECEIPT_REQ_ID"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                    .InnerJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.BIN_ID") == OOQL.CreateProperty("BIN.BIN_ID"))
                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                    .LeftJoin("ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                        & OOQL.CreateProperty("ISSUE_RECEIPT_REQ.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                    .LeftJoin("UNIT", "STOCK_UNIT")
                    .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
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
                    .On((OOQL.CreateProperty("REG_I_F.ITEM_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_D.ITEM_ID"))
                        &
                        (OOQL.CreateProperty("REG_I_F.ITEM_FEATURE_ID") ==
                         OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_D.ITEM_FEATURE_ID"))
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
                    .On((OOQL.CreateProperty("REG_I.ITEM_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_D.ITEM_ID"))
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
                // add by 08628 for P001-171023001 e
                    .Where((OOQL.AuthFilter("ISSUE_RECEIPT_REQ", "ISSUE_RECEIPT_REQ")) &
                            (OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo))) &
                           (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)) &
                           (OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ApproveStatus") == OOQL.CreateConstants("Y")));
            return queryNode;
        }

        #endregion
    }
}

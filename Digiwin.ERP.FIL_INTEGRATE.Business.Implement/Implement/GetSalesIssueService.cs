//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/9/25 16:58:20</CreateDate>
//<IssueNO>P001-170717001</IssueNO>
//<Description>获取销货出库单服务</Description>
//----------------------------------------------------------------  

using System.Collections.Generic;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    /// <summary>
    /// 获取销货出库单服务 
    /// </summary>
    [ServiceClass(typeof(IGetSalesIssueService))]
    [Description("获取销货出库单服务")]
    sealed class GetSalesIssueService : ServiceComponent, IGetSalesIssueService {
        /// <summary>
        /// 获取销货出库单服务
        /// </summary>
        /// <param name="programJobNo"></param>
        /// <param name="scanType"></param>
        /// <param name="status"></param>
        /// <param name="docNo"></param>
        /// <param name="siteNo"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        public DependencyObjectCollection GetSalesIssue(string programJobNo, string scanType, string status, string[] docNo, string siteNo, string ID) {

            #region 参数检查
            IInfoEncodeContainer infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

            if (Maths.IsEmpty(programJobNo)) {
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "program_job_no"));//‘入参【programJobNo】未传值’
            }
            if (Maths.IsEmpty(status)) {
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));
            }
            if (Maths.IsEmpty(docNo)) {
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "doc_no"));
            }
            if (Maths.IsEmpty(siteNo)) {
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "siteNo"));
            }
            #endregion

            //查询销货出库单信息
            QueryNode queryNode = GetSalesIssueNode(programJobNo, siteNo, docNo, status);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        /// <summary>
        /// 拼接查询语句
        /// </summary>
        /// <param name="programJobNo"></param>
        /// <param name="siteNo"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        private QueryNode GetSalesIssueNode(string programJobNo, string siteNo, string[] docNo, string status) {
            List<QueryProperty> selectList = GetSelectList(programJobNo, status);
            return OOQL.Select(selectList.ToArray()
                               )
                        .From("SALES_ISSUE", "SALES_ISSUE")
                        .InnerJoin("SALES_ISSUE.SALES_ISSUE_D", "SALES_ISSUE_D")
                        .On(OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_ID") == OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID"))
                        .InnerJoin("PLANT", "PLANT")
                        .On(OOQL.CreateProperty("SALES_ISSUE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                        .InnerJoin("ITEM", "ITEM")
                        .On(OOQL.CreateProperty("SALES_ISSUE_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                        .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                        .On(OOQL.CreateProperty("SALES_ISSUE_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                        .LeftJoin("WAREHOUSE", "WAREHOUSE")
                        .On(OOQL.CreateProperty("SALES_ISSUE_D.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                        .LeftJoin("WAREHOUSE.BIN", "BIN")
                        .On(OOQL.CreateProperty("SALES_ISSUE_D.BIN_ID") == OOQL.CreateProperty("BIN.BIN_ID"))
                        .LeftJoin("ITEM_LOT", "ITEM_LOT")
                        .On(OOQL.CreateProperty("SALES_ISSUE_D.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                        .LeftJoin("UNIT", "UNIT")
                        .On(OOQL.CreateProperty("SALES_ISSUE_D.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                        .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                        .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                            & OOQL.CreateProperty("SALES_ISSUE.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                        .LeftJoin("ITEM_SALES", "ITEM_SALES")
                        .On(OOQL.CreateProperty("ITEM_SALES.ITEM_ID") == OOQL.CreateProperty("SALES_ISSUE_D.ITEM_ID") &
                           OOQL.CreateProperty("ITEM_SALES.Owner_Org.ROid") == OOQL.CreateProperty("SALES_ISSUE.Owner_Org.ROid"))
                        .LeftJoin("UNIT", "STOCK_UNIT")
                        .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                        .Where(OOQL.AuthFilter("SALES_ISSUE", "SALES_ISSUE") & (
                            OOQL.CreateProperty("SALES_ISSUE.DOC_NO").In(OOQL.CreateDyncParameter("docNo", docNo))
                               & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                               & OOQL.CreateProperty("SALES_ISSUE.ApproveStatus") == OOQL.CreateConstants("N", GeneralDBType.String)
                               & OOQL.CreateProperty("SALES_ISSUE_D.BC_CHECK_STATUS") == OOQL.CreateConstants("1", GeneralDBType.String)));

        }

        /// <summary>
        /// 获取select集合,直接跟返回集合要求字段一致
        /// </summary>
        /// <returns></returns>
        private List<QueryProperty> GetSelectList(string programJobNo, string status) {
            List<QueryProperty> selectList = new List<QueryProperty>();
            selectList.Add(OOQL.CreateConstants("99", "enterprise_no"));
            selectList.Add(OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"));
            selectList.Add(OOQL.CreateConstants(programJobNo, "source_operation"));
            selectList.Add(OOQL.CreateProperty("SALES_ISSUE.DOC_NO", "source_no"));
            selectList.Add(OOQL.CreateArithmetic(OOQL.CreateConstants(programJobNo), OOQL.CreateConstants(status)
                           , ArithmeticOperators.Plus, "doc_type"));
            selectList.Add(OOQL.CreateProperty("SALES_ISSUE.DOC_DATE", "create_date"));
            selectList.Add(OOQL.CreateProperty("SALES_ISSUE_D.SequenceNumber", "seq"));
            selectList.Add(OOQL.CreateConstants(0, "doc_line_seq"));
            selectList.Add(OOQL.CreateConstants(0, "doc_batch_seq"));
            selectList.Add(OOQL.CreateConstants(string.Empty, "object_no"));

            selectList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            selectList.Add(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE", "item_feature_no"));
            selectList.Add(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "item_feature_name"));
            selectList.Add(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE", "warehouse_no"));
            selectList.Add(OOQL.CreateProperty("BIN.BIN_CODE", "storage_spaces_no"));
            selectList.Add(OOQL.CreateProperty("ITEM_LOT.LOT_CODE", "lot_no"));
            selectList.Add(OOQL.CreateProperty("SALES_ISSUE_D.BUSINESS_QTY", "doc_qty"));
            selectList.Add(OOQL.CreateConstants(0M, GeneralDBType.Decimal, "in_out_qty"));
            selectList.Add(OOQL.CreateProperty("UNIT.UNIT_CODE", "unit_no"));
            selectList.Add(OOQL.CreateProperty("ITEM_SALES.GENERAL_DEL_OVERRUN_RATE", "allow_error_rate"));
            selectList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            selectList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            selectList.Add(Formulas.Case(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL"), OOQL.CreateConstants("1", GeneralDBType.String),
                                      OOQL.CreateCaseArray(
                                          OOQL.CreateCaseItem(
                                              OOQL.CreateConstants("N", GeneralDBType.String),
                                              OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_typ"));
            selectList.Add(Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]{ OOQL.CreateProperty("SALES_ISSUE_D.ITEM_ID")
                                                  , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                                  , OOQL.CreateProperty("SALES_ISSUE_D.BUSINESS_UNIT_ID")
                                                  , OOQL.CreateConstants(1)}));  //单位转换率分母
            selectList.Add(Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]{ OOQL.CreateProperty("SALES_ISSUE_D.ITEM_ID")
                                          , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                          , OOQL.CreateProperty("SALES_ISSUE_D.BUSINESS_UNIT_ID")
                                          , OOQL.CreateConstants(0)}));//单位转换率分子
            selectList.Add(Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "inventory_unit")); //库存单位
            selectList.Add(OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"));
            selectList.Add(OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"));
            selectList.Add(OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"));
            return selectList;
        }
    }
}


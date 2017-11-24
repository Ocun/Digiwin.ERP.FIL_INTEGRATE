//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/9/25 16:58:20</CreateDate>
//<IssueNO>P001-170717001</IssueNO>
//<Description>获取销货出库单通知服务</Description>
//----------------------------------------------------------------  

using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    /// <summary>
    ///  获取销货出库单通知服务
    /// </summary>
    [ServiceClass(typeof(IGetSalesIssueListService))]
    [Description("获取销货出库单通知服务")]
    sealed class GetSalesIssueListService : ServiceComponent, IGetSalesIssueListService {
        /// <summary>
        /// 获取销货出库单通知服务
        /// </summary>
        /// <param name="programJobNo"></param>
        /// <param name="scanType"></param>
        /// <param name="status"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DependencyObjectCollection GetSalesIssueList(string programJobNo, string scanType, string status, string siteNo, DependencyObjectCollection condition) {

            #region 参数检查
            IInfoEncodeContainer infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

            if (Maths.IsEmpty(programJobNo)) {
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "program_job_no"));//‘入参【status】未传值’
            }
            if (Maths.IsEmpty(status)) {
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));//‘入参【status】未传值’//message A111201
            }
            if (Maths.IsEmpty(siteNo)) {
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));//‘‘入参【site_no】未传值’//message A111201
            }
            #endregion

            //查询销货出库单信息
            QueryNode queryNode = GetSalesIssueListNode(programJobNo, siteNo, condition);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        /// <summary>
        /// 拼接查询语句
        /// </summary>
        /// <param name="programJobNo"></param>
        /// <param name="siteNo"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        private QueryNode GetSalesIssueListNode(string programJobNo, string siteNo, DependencyObjectCollection condition) {
            QueryConditionGroup group = CreateCondition(siteNo, condition);
            return OOQL.Select(true,
                                OOQL.CreateProperty("SALES_ISSUE.DOC_NO", "doc_no"),
                                OOQL.CreateProperty("SALES_ISSUE.DOC_DATE", "create_date"),
                                OOQL.CreateProperty("CUSTOMER.CUSTOMER_NAME", "customer_name"),
                                OOQL.CreateConstants(programJobNo, GeneralDBType.String, "program_job_no"),
                                OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME", "employee_name"),
                                OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"))
                        .From("SALES_ISSUE", "SALES_ISSUE")
                        .InnerJoin("PLANT", "PLANT")
                        .On(OOQL.CreateProperty("SALES_ISSUE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                        .LeftJoin("CUSTOMER", "CUSTOMER")
                        .On(OOQL.CreateProperty("SALES_ISSUE.SHIP_TO_CUSTOMER_ID") == OOQL.CreateProperty("CUSTOMER.CUSTOMER_ID"))
                        .LeftJoin("EMPLOYEE", "EMPLOYEE")
                        .On(OOQL.CreateProperty("SALES_ISSUE.Owner_Emp") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"))
                        .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                        .On(OOQL.CreateProperty("SALES_ISSUE.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"))
                        .LeftJoin("SALES_ISSUE.SALES_ISSUE_D", "SALES_ISSUE_D")
                        .On(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID") == OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_ID"))
                        .Where(OOQL.AuthFilter("SALES_ISSUE", "SALES_ISSUE") & group);
        }

        /// <summary>
        /// 拼接查询条件
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        private QueryConditionGroup CreateCondition(string siteNo, DependencyObjectCollection condition) {
            QueryConditionGroup group = OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                                           & OOQL.CreateProperty("SALES_ISSUE.ApproveStatus") == OOQL.CreateConstants("N", GeneralDBType.String)
                                           & OOQL.CreateProperty("SALES_ISSUE_D.BC_CHECK_STATUS") == OOQL.CreateConstants("1", GeneralDBType.String);
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("SALES_ISSUE.DOC_NO", "SALES_ISSUE.DOC_DATE", new string[] { "1", "2", "3", "4", "6" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            return group;
        }

    }
}

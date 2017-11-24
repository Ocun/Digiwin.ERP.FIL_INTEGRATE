//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>liwei1</author>
//<createDate>2017/07/19</createDate>
//<IssueNo>P001-170717001</IssueNo>
//<description>获取寄售订单通知服务</description>
//----------------------------------------------------------------

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetSalesOrderListService))]
    [Description("获取寄售订单通知服务")]
    public class GetSalesOrderListService : ServiceComponent, IGetSalesOrderListService {

        #region 接口方法
        /// <summary>
        /// 根据传入的条码，获取相应的寄售订单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.有条码 2.无条码</param>
        /// <param name="status">执行动作A.新增  S.过帐</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="condition">/多笔记录，包含两个属性①seq   numeric  项次//1.单号 2.日期  3.人员 4.部门 5.供货商6. 客户 7.料号②value  String 栏位值</param>
        /// <returns></returns>
        public DependencyObjectCollection GetSalesOrderList(string programJobNo, string scanType, string status, string siteNo, DependencyObjectCollection condition) {

            #region 参数检查
            if (Maths.IsEmpty(programJobNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "program_job_no"));//‘入参【program_job_no】未传值’
            }
            if (Maths.IsEmpty(status)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));//‘入参【status】未传值’//message A111201
            }
            if (Maths.IsEmpty(siteNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));//‘‘入参【site_no】未传值’//message A111201
            }
            #endregion

            //查询寄售订单信息
            return GetSalesOrder(siteNo, programJobNo, condition);

        }
        #endregion

        #region 业务方法
        /// <summary>
        /// 获取寄售订单数据
        /// </summary>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private DependencyObjectCollection GetSalesOrder(string siteNo, string programJobNo, DependencyObjectCollection condition){
            QueryNode queryNode =
                OOQL.Select(true,
                    OOQL.CreateProperty("SALES_ORDER_DOC.DOC_NO", "doc_no"),
                    OOQL.CreateProperty("SALES_ORDER_DOC.DOC_DATE", "create_date"),
                    Formulas.IsNull(
                        OOQL.CreateProperty("CUSTOMER.CUSTOMER_NAME"),
                        OOQL.CreateConstants(string.Empty), "customer_name"),
                    OOQL.CreateConstants(programJobNo, "program_job_no"),
                    Formulas.IsNull(
                        OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"),
                        OOQL.CreateConstants(string.Empty), "employee_name"),
                    Formulas.IsNull(
                        OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_CODE"),
                        OOQL.CreateConstants(string.Empty), "main_organization"))
                    .From("SALES_ORDER_DOC", "SALES_ORDER_DOC")
                    .InnerJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D", "SALES_ORDER_DOC_D")
                    .On((OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_ID") == OOQL.CreateProperty("SALES_ORDER_DOC.SALES_ORDER_DOC_ID")))
                    .InnerJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD")
                    .On((OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_D_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_D_ID")))
                    .InnerJoin("PLANT", "PLANT")
                    .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_SD.DELIVERY_PARTNER_ID.ROid")))
                    .InnerJoin("SALES_CENTER", "SALES_CENTER")
                    .On((OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Org.ROid")))
                    .LeftJoin("CUSTOMER", "CUSTOMER")
                    .On((OOQL.CreateProperty("CUSTOMER.CUSTOMER_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.CUSTOMER_ID")))
                    .LeftJoin("EMPLOYEE", "EMPLOYEE")
                    .On((OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Emp")))
                    .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                    .On((OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Dept")));

            QueryConditionGroup group =                  
                ((OOQL.CreateProperty("SALES_ORDER_DOC.ApproveStatus") == OOQL.CreateConstants("Y"))
               & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo))
               & (OOQL.CreateProperty("SALES_ORDER_DOC.CATEGORY") == OOQL.CreateConstants("2B"))
               & (OOQL.CreateProperty("SALES_ORDER_DOC.CLOSE") == OOQL.CreateConstants("0")));

            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("SALES_ORDER_DOC.DOC_NO", "SALES_ORDER_DOC.DOC_DATE", new string[] { "1", "2", "3", "4", "6" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            queryNode = ((JoinOnNode)queryNode).Where(OOQL.AuthFilter("SALES_ORDER_DOC", "SALES_ORDER_DOC") & (group));
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }
        #endregion
    }
}

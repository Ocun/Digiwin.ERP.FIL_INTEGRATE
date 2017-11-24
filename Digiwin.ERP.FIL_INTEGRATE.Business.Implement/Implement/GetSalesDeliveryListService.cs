//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-04</createDate>
//<description>获取销货单通知服务实现</description>
//---------------------------------------------------------------- 
//20170104 modi by wangyq for P001-161215001
//20170328 modi by wangyq for P001-170327001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetSalesDeliveryListService))]
    [Description("获取销货单通知服务")]
    public class GetSalesDeliveryListService : ServiceComponent, IGetSalesDeliveryListService {

        #region IGetSalesDeliveryListService 成员
        /// <summary>
        /// 根据传入的条码，获取相应的销货单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.有条码 2.无条码</param>
        /// <param name="status">执行动作A.新增  S.过帐</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetSalesDeliveryList(string programJobNo, string scanType, string status, string siteNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {

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

            //查询销货单信息
            QueryNode queryNode = GetSalesDeliveryQueryNode(siteNo, programJobNo, condition);//20170328 modi by wangyq for P001-170327001 添加参数condition
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }
        #endregion

        #region 业务方法
        /// <summary>
        /// 获取销货单QueryNode
        /// </summary>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private QueryNode GetSalesDeliveryQueryNode(string siteNo, string programJobNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            QueryNode queryNode =//20170328 modi by wangyq for P001-170327001 old: return 
                 OOQL.Select(true,//20170104 add by wangyq for P001-161215001
                                     OOQL.CreateProperty("SALES_DELIVERY.DOC_NO", "doc_no"),
                                     OOQL.CreateProperty("SALES_DELIVERY.DOC_DATE", "create_date"),
                                     OOQL.CreateProperty("CUSTOMER.CUSTOMER_NAME", "customer_name"),
                                     OOQL.CreateConstants(programJobNo, GeneralDBType.String, "program_job_no"),
                                     Formulas.IsNull(
                                             OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"),
                                             OOQL.CreateConstants(string.Empty, GeneralDBType.String), "employee_name"),
                                     OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_CODE", "main_organization"))
                                 .From("SALES_DELIVERY", "SALES_DELIVERY")
                                 .InnerJoin("SALES_CENTER", "SALES_CENTER")
                                 .On((OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_ID") == OOQL.CreateProperty("SALES_DELIVERY.Owner_Org.ROid")))
                                 .InnerJoin("PLANT", "PLANT")
                                 .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("SALES_DELIVERY.PLANT_ID")))
                                 .LeftJoin("CUSTOMER", "CUSTOMER")
                                 .On((OOQL.CreateProperty("CUSTOMER.CUSTOMER_ID") == OOQL.CreateProperty("SALES_DELIVERY.CUSTOMER_ID")))
                                 .LeftJoin("EMPLOYEE", "EMPLOYEE")
                                 .On((OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("SALES_DELIVERY.Owner_Emp")))
                //20170104 add by wangyq for P001-161215001  ================begin=====================
                                 .LeftJoin("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D")
                                 .On(OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_ID") == OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_ID"))
                //20170328 add by wangyq for P001-170327001  ================begin==============
                       .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                       .On(OOQL.CreateProperty("SALES_DELIVERY.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"));
            //20170328 add by wangyq for P001-170327001  ================end==============
            //20170104 add by wangyq for P001-161215001  ================end=====================
            //.Where( //20170328 mark by wangyq for P001-170327001
            //20170104 add by wangyq for P001-161215001  ================begin=====================
            //OOQL.AuthFilter("SALES_DELIVERY", "SALES_DELIVERY") &//20170328 mark by wangyq for P001-170327001
            QueryConditionGroup group = //20170328 add by wangyq for P001-170327001                            
           (OOQL.CreateProperty("SALES_DELIVERY_D.ISSUED") == OOQL.CreateConstants("0", GeneralDBType.String) &
                //20170104 add by wangyq for P001-161215001  ================end=====================
                                     (OOQL.CreateProperty("SALES_DELIVERY.ApproveStatus") == OOQL.CreateConstants("Y", GeneralDBType.String))
                                   & (OOQL.CreateProperty("SALES_DELIVERY.CATEGORY") == OOQL.CreateConstants("24", GeneralDBType.String))
                                   & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo, GeneralDBType.String)));
            //); //20170328 mark by wangyq for P001-170327001
            //20170328 add by wangyq for P001-170327001  ================begin==============
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("SALES_DELIVERY.DOC_NO", "SALES_DELIVERY.DOC_DATE", new string[] { "1", "2", "3", "4", "6" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            queryNode = ((JoinOnNode)queryNode).Where(OOQL.AuthFilter("SALES_DELIVERY", "SALES_DELIVERY") & (group));
            return queryNode;
            //20170328 add by wangyq for P001-170327001  ================end==============
        }
        #endregion
    }
}

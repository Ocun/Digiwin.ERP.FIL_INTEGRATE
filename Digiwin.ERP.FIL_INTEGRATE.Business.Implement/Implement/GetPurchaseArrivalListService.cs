//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-03</createDate>
//<description>获取领退料通知服务 实现</description>
//----------------------------------------------------------------
//20170328 modi by wangyq for P001-170327001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取领退料通知服务
    /// </summary>
    [ServiceClass(typeof(IGetPurchaseArrivalListService))]
    [Description("获取领退料通知服务")]
    public class GetPurchaseArrivalListService : ServiceComponent, IGetPurchaseArrivalListService {
        #region 接口方法
        /// <summary>
        /// 查询领料出库单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetPurchaseArrival(string programJobNo, string scanType, string status, string siteNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
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

            //查询领料出库单信息
            QueryNode queryNode = GetPurchaseArrivalQueryNode(siteNo, programJobNo, condition);//20170328 modi by wangyq for P001-170327001 添加参数condition

            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 获取领料出库单查询信息
        /// </summary>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private QueryNode GetPurchaseArrivalQueryNode(string siteNo, string programJobNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            QueryNode queryNode =
               OOQL.Select(OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO", "doc_no"),
                           OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_DATE", "create_date"),
                           OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME", "customer_name"),
                           OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE", "main_organization"),
                           OOQL.CreateConstants(programJobNo, "program_job_no"),
                           Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"), OOQL.CreateConstants(string.Empty), "employee_name"))
                    .From("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                    .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                    .On(OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.ROid"))
                    .InnerJoin("SUPPLIER", "SUPPLIER")
                    .On(OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.SUPPLIER_ID"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.RECEIVE_Owner_Org.ROid"))
                    .LeftJoin("EMPLOYEE", "EMPLOYEE")
                    .On(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Emp"))
                //20170328 add by wangyq for P001-170327001  ================begin==============
                      .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                      .On(OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"));
            //20170328 add by wangyq for P001-170327001  ================end==============
            //.Where((OOQL.AuthFilter("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")) &//20170328 mark by wangyq for P001-170327001
            QueryConditionGroup group =//20170328 add by wangyq for P001-170327001
                          ((OOQL.CreateProperty("PURCHASE_ARRIVAL.CATEGORY") == OOQL.CreateConstants("36")) &
                           (OOQL.CreateProperty("PURCHASE_ARRIVAL.ApproveStatus") == OOQL.CreateConstants("Y")) &
                           (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)) &
                           (OOQL.CreateProperty("PURCHASE_ARRIVAL.RECEIPTED_STATUS").In(OOQL.CreateConstants("1"), OOQL.CreateConstants("2")))
                           );
            //);//20170328 mark by wangyq for P001-170327001
            //20170328 add by wangyq for P001-170327001  ================begin==============
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("PURCHASE_ARRIVAL.DOC_NO", "PURCHASE_ARRIVAL.DOC_DATE", new string[] { "1", "2", "3", "4", "5" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            queryNode = ((JoinOnNode)queryNode).Where(OOQL.AuthFilter("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL") & (group));
            //20170328 add by wangyq for P001-170327001  ================end==============
            return queryNode;
        }
        #endregion
    }
}

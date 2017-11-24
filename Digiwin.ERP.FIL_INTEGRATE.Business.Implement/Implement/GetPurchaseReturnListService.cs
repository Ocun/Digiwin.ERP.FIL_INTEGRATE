//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-2-10</createDate>
//<IssueNo>P001-170207001</IssueNo>
//<description>获取采购退货单通知服务实现</description>
//----------------------------------------------------------------
//20170328 modi by wangyq for P001-170327001

using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;
using Digiwin.Common.Query2;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetPurchaseReturnListService))]
    [Description("获取采购退货单通知服务实现")]
    class GetPurchaseReturnListService : ServiceComponent, IGetPurchaseReturnListService {

        #region IGetPurchaseReturnListService 成员
        /// <summary>
        /// 获取采购退货单通知
        /// </summary>
        /// <param name="programJobNo">作业编号  4-1.采购退货单</param>
        /// <param name="scanType">扫描类型  1.有条码 2.无条码</param>
        /// <param name="status">执行动作  A.新增  S.过帐</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetPurchaseReturnList(string programJobNo, string scanType, string status, string siteNo
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

            //查询采购退货单
            QueryNode queryNode = GetPurchaseReturnListNode(programJobNo, siteNo, condition);//20170328 modi by wangyq for P001-170327001 添加参数condition
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法
        /// <summary>
        /// 查询采购退货单
        /// </summary>
        /// <param name="programJobNo">作业编号  4-1.采购退货单</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public QueryNode GetPurchaseReturnListNode(string programJobNo, string siteNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE", "main_organization"),
                                         OOQL.CreateProperty("PURCHASE_RETURN.DOC_NO", "doc_no"),
                                         OOQL.CreateProperty("PURCHASE_RETURN.DOC_DATE", "create_date"),
                                         OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME", "customer_name"),
                                         OOQL.CreateConstants(programJobNo, "program_job_no"),
                                         Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"), OOQL.CreateConstants(string.Empty), "employee_name"))
                           .From("PURCHASE_RETURN", "PURCHASE_RETURN")
                           .InnerJoin("PLANT", "PLANT")
                           .On(OOQL.CreateProperty("PURCHASE_RETURN.PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                           .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                           .On(OOQL.CreateProperty("PURCHASE_RETURN.Owner_Org.ROid") == OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID"))
                           .InnerJoin("SUPPLIER", "SUPPLIER")
                           .On(OOQL.CreateProperty("PURCHASE_RETURN.SUPPLIER_ID") == OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID"))
                           .LeftJoin("EMPLOYEE", "EMPLOYEE")
                           .On(OOQL.CreateProperty("PURCHASE_RETURN.Owner_Emp") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"))
                //20170328 add by wangyq for P001-170327001  ================begin==============
                      .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                      .On(OOQL.CreateProperty("PURCHASE_RETURN.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"));
            //20170328 add by wangyq for P001-170327001  ================end==============
            //.Where((OOQL.AuthFilter("PURCHASE_RETURN", "PURCHASE_RETURN")) & //20170328 mark by wangyq for P001-170327001
            QueryConditionGroup group =//20170328 mark by wangyq for P001-170327001
                           (OOQL.CreateProperty("PURCHASE_RETURN.CATEGORY") == OOQL.CreateConstants("39")
                           & OOQL.CreateProperty("PURCHASE_RETURN.ApproveStatus") == OOQL.CreateConstants("Y")
                           & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                           & OOQL.CreateProperty("PURCHASE_RETURN.ISSUE_STATUS").In(OOQL.CreateConstants("1"),
                                                                                     OOQL.CreateConstants("2")));
            //); //20170328 mark by wangyq for P001-170327001
            //20170328 add by wangyq for P001-170327001  ================begin==============
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("PURCHASE_RETURN.DOC_NO", "PURCHASE_RETURN.DOC_DATE", new string[] { "1", "2", "3", "4", "5" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            node = ((JoinOnNode)node).Where(OOQL.AuthFilter("PURCHASE_RETURN", "PURCHASE_RETURN") & (group));
            //20170328 add by wangyq for P001-170327001  ================end==============
            return node;
        }
        #endregion

    }
}

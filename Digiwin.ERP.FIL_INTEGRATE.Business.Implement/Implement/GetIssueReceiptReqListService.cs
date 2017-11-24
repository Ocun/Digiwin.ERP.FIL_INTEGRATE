//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/12/23 13:49:37</createDate>
//<IssueNo>P001-161215001</IssueNo>
//<description>获取领料申请单通知</description>
//20170104 modi by wangyq for P001-161215001
//20170328 modi by wangyq for P001-170327001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [SingleGetCreator]
    [ServiceClass(typeof(IGetIssueReceiptReqListService))]
    [Description("获取领料申请单通知")]
    public sealed class GetIssueReceiptReqListService : ServiceComponent, IGetIssueReceiptReqListService {
        #region IGetTransferReqListService 成员
        /// <summary>
        /// 获取领料申请单通知
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetIssueReceiptReqList(string programJobNo, string scanType, string status, string siteNo
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

            return this.GetService<IQueryService>().ExecuteDependencyObject(GetIssueReceiptReqListNode(siteNo, programJobNo, condition));//20170328 modi by wangyq for P001-170327001 添加参数condition
        }

        #endregion

        #region 自定义方法

        private QueryNode GetIssueReceiptReqListNode(string siteNo, string programJobNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            QueryNode node = OOQL.Select(
                    OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_NO", "doc_no"),
                    OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_DATE", "create_date"),
                    Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME"), new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("ISSUE_RECEIPT_REQ.SOURCE_ID.RTK")==OOQL.CreateConstants("WORK_CENTER")
                            ,OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_NAME"))
                    }), OOQL.CreateConstants(string.Empty), "customer_name"),
                    OOQL.CreateConstants(programJobNo, "program_job_no"),
                    Formulas.IsNull(
                            OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"),
                            OOQL.CreateConstants(string.Empty), "employee_name"),
                    OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"))
                .From("ISSUE_RECEIPT_REQ", "ISSUE_RECEIPT_REQ")
                .InnerJoin("PLANT", "PLANT")
                .On((OOQL.CreateProperty("ISSUE_RECEIPT_REQ.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                .LeftJoin("WORK_CENTER")
                .On((OOQL.CreateProperty("ISSUE_RECEIPT_REQ.SOURCE_ID.ROid") == OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_ID")))
                .LeftJoin("SUPPLIER")
                .On((OOQL.CreateProperty("ISSUE_RECEIPT_REQ.SOURCE_ID.ROid") == OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID")))
                .LeftJoin("EMPLOYEE", "EMPLOYEE")
                .On((OOQL.CreateProperty("ISSUE_RECEIPT_REQ.Owner_Emp") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID")))
                //20170328 add by wangyq for P001-170327001  ================begin==============
                      .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                      .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"));
            //20170328 add by wangyq for P001-170327001  ================end==============
            //.Where((OOQL.AuthFilter("ISSUE_RECEIPT_REQ", "ISSUE_RECEIPT_REQ")) &//20170328 mark by wangyq for P001-170327001
            QueryConditionGroup group =//20170328 add by wangyq for P001-170327001
                     (OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ApproveStatus") == OOQL.CreateConstants("Y")
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                    & OOQL.CreateProperty("ISSUE_RECEIPT_REQ.CLOSE") == OOQL.CreateConstants("0", GeneralDBType.String)//20170104 add by wangyq for P001-161215001
                    );
            //);//20170328 mark by wangyq for P001-170327001
            //20170328 add by wangyq for P001-170327001  ================begin==============
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("ISSUE_RECEIPT_REQ.DOC_NO", "ISSUE_RECEIPT_REQ.DOC_DATE", new string[] { "1", "2", "3", "4", "5" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            node = ((JoinOnNode)node).Where(OOQL.AuthFilter("ISSUE_RECEIPT_REQ", "ISSUE_RECEIPT_REQ") & (group));
            //20170328 add by wangyq for P001-170327001  ================end==============
            return node;
        }

        #endregion
    }
}

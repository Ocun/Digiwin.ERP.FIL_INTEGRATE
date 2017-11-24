//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-03</createDate>
//<description>获取领退料通知服务 实现</description>
//----------------------------------------------------------------
//20170328 modi by wangyq for P001-170327001
//20170925 modi by wangyq for P001-170717001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取领退料通知服务
    /// </summary>
    [ServiceClass(typeof(IGetIssueReceiptListService))]
    [Description("获取领退料通知服务")]
    public class GetIssueReceiptListService : ServiceComponent, IGetIssueReceiptListService {
        #region 接口方法
        /// <summary>
        /// 查询领料出库单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetIssueReceipt(string programJobNo, string scanType, string status, string siteNo,
            DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            #region 参数检查
            if (Maths.IsEmpty(programJobNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                //‘入参【program_job_no】未传值’
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "program_job_no"));
            }
            if (Maths.IsEmpty(status)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                //‘入参【status】未传值’
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));

            }
            if (Maths.IsEmpty(siteNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));//‘入参【site_no】未传值’
            }
            #endregion

            QueryNode queryNode;
            if (status == "A") {
                //查询领料出库单信息
                queryNode = GetMOQueryNode(siteNo, programJobNo, condition);//20170328 modi by wangyq for P001-170327001 添加参数condition
            } else {
                //查询领料出库单信息
                queryNode = GetIssueReceiptQueryNode(siteNo, programJobNo, condition);//20170328 modi by wangyq for P001-170327001 添加参数condition
            }
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 获取工单信息查询信息
        /// </summary>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="programJobNo">作业编号</param>        
        /// <returns></returns>
        private QueryNode GetMOQueryNode(string siteNo, string programJobNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            QueryNode queryNode =
                OOQL.Select(true, OOQL.CreateProperty("MO.DOC_NO", "doc_no"),
                            OOQL.CreateProperty("MO.DOC_DATE", "create_date"),
                            Formulas.Case(null,
                                         OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem((OOQL.CreateProperty("MO.SOURCE_ID.RTK") == OOQL.CreateConstants("WORK_CENTER")),
                                                              OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_NAME"))),
                                         "customer_name"),
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"),
                            OOQL.CreateConstants(programJobNo, "program_job_no"),
                            Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"), OOQL.CreateConstants(string.Empty), "employee_name"))
                     .From("MO", "MO")
                     .LeftJoin("MO.MO_D", "MO_D")
                     .On(OOQL.CreateProperty("MO_D.MO_ID") == OOQL.CreateProperty("MO.MO_ID"))
                     .LeftJoin("PLANT", "PLANT")
                     .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("MO.Owner_Org.ROid"))
                     .LeftJoin("WORK_CENTER", "WORK_CENTER")
                     .On(OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_ID") == OOQL.CreateProperty("MO.SOURCE_ID.ROid"))
                     .LeftJoin("SUPPLIER", "SUPPLIER")
                     .On(OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("MO.SOURCE_ID.ROid"))
                     .LeftJoin("EMPLOYEE", "EMPLOYEE")
                     .On(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("MO.Owner_Emp"))
                     .LeftJoin("DOC", "DOC")
                     .On(OOQL.CreateProperty("DOC.DOC_ID") == OOQL.CreateProperty("MO.DOC_ID"))
                //20170328 add by wangyq for P001-170327001  ================begin==============
                      .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                      .On(OOQL.CreateProperty("MO.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"));
            //20170328 add by wangyq for P001-170327001  ================end==============
            //.Where(//20170328 mark by wangyq for P001-170327001
            QueryConditionGroup group = //20170328 add by wangyq for P001-170327001
                //(OOQL.AuthFilter("MO", "MO")) & (Where(//20170328 mark by wangyq for P001-170327001
                        (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)) &
                         (OOQL.CreateProperty("MO.ApproveStatus") == OOQL.CreateConstants("Y")) &
                         (OOQL.CreateProperty("DOC.CATEGORY").In(OOQL.CreateConstants("51"),
                                                                 OOQL.CreateConstants("52"),
                                                                 OOQL.CreateConstants("53"),
                                                                 OOQL.CreateConstants("54"))) &
                         (OOQL.CreateProperty("MO.LOT_MO_FLAG") == OOQL.CreateConstants(false)) &
                         (OOQL.CreateProperty("MO.STATUS").In(OOQL.CreateConstants("1"),
                                                              OOQL.CreateConstants("2"),
                                                              OOQL.CreateConstants("3"))) &
                         ((OOQL.CreateConstants(programJobNo) == OOQL.CreateConstants("7") & OOQL.CreateProperty("MO_D.REQUIRED_QTY") > OOQL.CreateProperty("MO_D.ISSUED_QTY")) |
                          (OOQL.CreateConstants(programJobNo) == OOQL.CreateConstants("8") & OOQL.CreateProperty("MO_D.ISSUED_QTY") > OOQL.CreateConstants(0)));
            //));//20170328 mark by wangyq for P001-170327001
            //20170328 add by wangyq for P001-170327001  ================begin==============
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("MO.DOC_NO", "MO.DOC_DATE", new string[] { "1", "2", "3", "4", "5" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            queryNode = ((JoinOnNode)queryNode).Where(OOQL.AuthFilter("MO", "MO") & (group));
            //20170328 add by wangyq for P001-170327001  ================end==============
            return queryNode;
        }


        /// <summary>
        /// 获取领料出库单查询信息
        /// </summary>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private QueryNode GetIssueReceiptQueryNode(string siteNo, string programJobNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            QueryNode queryNode =
               OOQL.Select(true, OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO", "doc_no"),//20170925 modi by wangyq for P001-170717001 添加distinct因为后面关联加了单身
                           OOQL.CreateProperty("ISSUE_RECEIPT.DOC_DATE", "create_date"),
                           OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"),
                           Formulas.Case(null,
                                         OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem((OOQL.CreateProperty("ISSUE_RECEIPT.SOURCE_ID.RTK") == OOQL.CreateConstants("WORK_CENTER")),
                                                              OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_NAME"))),
                                         "customer_name"),
                           OOQL.CreateConstants(programJobNo, "program_job_no"),
                           Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"), OOQL.CreateConstants(string.Empty), "employee_name"))
                    .From("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                //20170925 add by wangyq for P001-170717001  ================begin==============
                    .InnerJoin("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID"))
                //20170925 add by wangyq for P001-170717001  ================end==============
                    .LeftJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.ROid"))
                    .LeftJoin("WORK_CENTER", "WORK_CENTER")
                    .On(OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_ID") == OOQL.CreateProperty("ISSUE_RECEIPT.SOURCE_ID.ROid"))
                    .LeftJoin("SUPPLIER", "SUPPLIER")
                    .On(OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("ISSUE_RECEIPT.SOURCE_ID.ROid"))
                    .LeftJoin("EMPLOYEE", "EMPLOYEE")
                    .On(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Emp"))
                //20170328 add by wangyq for P001-170327001  ================begin==============
                     .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                     .On(OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"));
            //20170328 add by wangyq for P001-170327001  ================end==============
            //.Where((OOQL.AuthFilter("ISSUE_RECEIPT", "ISSUE_RECEIPT")) &//20170328 mark by wangyq for P001-170327001
            //20170925 modi by wangyq for P001-170717001  =============begin===============
            //QueryConditionGroup group =//20170328 add by wangyq for P001-170327001             
            //               (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)) &
            //    //(OOQL.CreateProperty("ISSUE_RECEIPT.ApproveStatus") == OOQL.CreateConstants("N")) &//20170925 mark by wangyq for P001-170717001
            //               ((OOQL.CreateConstants(programJobNo) == OOQL.CreateConstants("7") & OOQL.CreateProperty("ISSUE_RECEIPT.CATEGORY") == OOQL.CreateConstants("56")) |
            //                (OOQL.CreateConstants(programJobNo) == OOQL.CreateConstants("8") & OOQL.CreateProperty("ISSUE_RECEIPT.CATEGORY") == OOQL.CreateConstants("57")));
            ////);//20170328 mark by wangyq for P001-170327001

            QueryConditionGroup group = (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)) &
                           OOQL.CreateProperty("ISSUE_RECEIPT.ApproveStatus") == OOQL.CreateConstants("N");
            switch (programJobNo) {
                case "7-5":
                    group &= OOQL.CreateProperty("ISSUE_RECEIPT_D.BC_CHECK_STATUS") == OOQL.CreateConstants("1")
                        & OOQL.CreateProperty("ISSUE_RECEIPT.CATEGORY") == OOQL.CreateConstants("56");
                    break;
                case "7":
                    group &= OOQL.CreateProperty("ISSUE_RECEIPT.CATEGORY") == OOQL.CreateConstants("56");
                    break;
                case "8":
                    group &= OOQL.CreateProperty("ISSUE_RECEIPT.CATEGORY") == OOQL.CreateConstants("57");
                    break;
                default:
                    break;
            }
            //20170925 modi by wangyq for P001-170717001  =============end===============

            //20170328 add by wangyq for P001-170327001  ================begin==============
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("ISSUE_RECEIPT.DOC_NO", "ISSUE_RECEIPT.DOC_DATE", new string[] { "1", "2", "3", "4", "5" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            queryNode = ((JoinOnNode)queryNode).Where(OOQL.AuthFilter("ISSUE_RECEIPT", "ISSUE_RECEIPT") & (group));
            //20170328 add by wangyq for P001-170327001  ================end==============
            return queryNode;
        }
        #endregion
    }
}

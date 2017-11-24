﻿//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-03</createDate>
//<description>获取出入单通知服务 实现</description>
//----------------------------------------------------------------
//20170328 modi by wangyq for P001-170327001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取出入单通知服务
    /// </summary>
    [ServiceClass(typeof(IGetTransactionDocListService))]
    [Description("获取出入单通知服务")]
    public class GetTransactionDocListService : ServiceComponent, IGetTransactionDocListService {
        #region 接口方法
        /// <summary>
        /// 查询调出单
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetTransactionDoc(string programJobNo, string scanType, string status, string siteNo
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
            QueryNode queryNode = GetTransactionDocQueryNode(siteNo, programJobNo, condition);//20170328 modi by wangyq for P001-170327001 添加参数condition
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 获取库存交易单查询信息
        /// </summary>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private QueryNode GetTransactionDocQueryNode(string siteNo, string programJobNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            QueryConditionGroup conditionGroup = (OOQL.AuthFilter("TRANSACTION_DOC", "TRANSACTION_DOC")) &
                                                (((OOQL.CreateProperty("TRANSACTION_DOC.CATEGORY") == OOQL.CreateConstants("11")) &
                                                (OOQL.CreateProperty("TRANSACTION_DOC.ApproveStatus") == OOQL.CreateConstants("N")) &
                                                (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo))));
            if (programJobNo == "11") {
                //箱条码
                conditionGroup &= (OOQL.CreateProperty("TRANSACTION_DOC.STOCK_ACTION") == OOQL.CreateConstants(-1));
            } else if (programJobNo == "12") {
                //单据条码
                conditionGroup &= (OOQL.CreateProperty("TRANSACTION_DOC.STOCK_ACTION") == OOQL.CreateConstants(1));
            }
            QueryNode queryNode =
               OOQL.Select(true,
                           OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO", "doc_no"),
                           OOQL.CreateProperty("TRANSACTION_DOC.DOC_DATE", "create_date"),
                           Formulas.IsNull(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_NAME"), OOQL.CreateConstants(string.Empty), "customer_name"),
                           OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"),
                           OOQL.CreateConstants(programJobNo, "program_job_no"),
                           Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"), OOQL.CreateConstants(string.Empty), "employee_name"))
                    .From("TRANSACTION_DOC", "TRANSACTION_DOC")
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.ROid"))
                    .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                    .On(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID") == OOQL.CreateProperty("TRANSACTION_DOC.Owner_Dept"))
                    .LeftJoin("EMPLOYEE", "EMPLOYEE")
                    .On(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("TRANSACTION_DOC.Owner_Emp"));
            //.Where(conditionGroup);//20170328 mark by wangyq for P001-170327001
            //20170328 add by wangyq for P001-170327001  ================begin==============
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("TRANSACTION_DOC.DOC_NO", "TRANSACTION_DOC.DOC_DATE", new string[] { "1", "2", "3", "4" });
                conditionGroup = UtilsClass.CreateNewConditionByParameter(conditionGroup, condition, conPropertyEntity);
            }
            queryNode = ((JoinOnNode)queryNode).Where(conditionGroup);
            //20170328 add by wangyq for P001-170327001  ================end==============
            return queryNode;
        }
        #endregion
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>shenbao</author>
//<createDate>2017/08/01 13:49:37</createDate>
//<IssueNo>P001-170717001</IssueNo>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;
using Digiwin.Common.Query2;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetMOListService))]
    [Description("获取未入库工单通知服务")]
    sealed class GetMOListService : ServiceComponent, IGetMOListService {
        /// <summary>
        /// 获取未入库工单通知服务
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetMOList(string programJobNo, string scanType, string status, string siteNo
            , DependencyObjectCollection condition
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

            return this.GetService<IQueryService>().ExecuteDependencyObject(GetMOListNode(siteNo, programJobNo, condition));
        }

        public QueryNode GetMOListNode(string siteNo, string programJobNo
            , DependencyObjectCollection condition) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("MO.DOC_NO", "doc_no"),
                    OOQL.CreateProperty("MO.DOC_DATE", "create_date"),
                    OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_NAME", "customer_name"),
                    OOQL.CreateConstants(programJobNo,GeneralDBType.String, "program_job_no"),
                    Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"), OOQL.CreateConstants(string.Empty), "employee_name"),
                    OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization")
                )
                .From("MO", "MO")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("MO.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("MO.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("MO.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("MO.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("WORK_CENTER")
                .On(OOQL.CreateProperty("MO.SOURCE_ID.RTK") == OOQL.CreateConstants("WORK_CENTER")
                    & OOQL.CreateProperty("MO.SOURCE_ID.ROid") == OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_ID"))
                .LeftJoin("ADMIN_UNIT")
                .On(OOQL.CreateProperty("MO.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"))
                .LeftJoin("EMPLOYEE")
                .On(OOQL.CreateProperty("MO.Owner_Emp") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"));

            QueryConditionGroup group =
                     (OOQL.CreateProperty("MO.ApproveStatus") == OOQL.CreateConstants("Y")
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                    & OOQL.CreateProperty("MO.STATUS").NotIn(OOQL.CreateConstants("Y", GeneralDBType.String), OOQL.CreateConstants("y", GeneralDBType.String))
                    & OOQL.CreateProperty("MO.RECEIPT_REQ_CONTROL")==OOQL.CreateConstants(false)
                    );
            if (condition != null && condition.Count > 0) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("MO.DOC_NO", "MO.DOC_DATE", new string[] { "1", "2", "3", "4" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }

            node = ((JoinOnNode)node).Where(OOQL.AuthFilter("MO", "MO") & (group));

            return node;
        }
    }
}

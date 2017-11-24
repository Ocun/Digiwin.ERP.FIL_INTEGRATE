//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-3-27</createDate>
//<IssueNo>P001-170316001</IssueNo>
//<description>获取待到货订单通知服务实现</description>
//----------------------------------------------------------------
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetFILPurchaseArrivalListService))]
    [Description("获取待到货订单通知服务 实现")]
    public class GetFILPurchaseArrivalListService : ServiceComponent, IGetFILPurchaseArrivalListService {

        #region IGetFILPurchaseArrivalListService 成员
        /// <summary>
        /// 查询待到货订单
        /// </summary>
        /// <param name="programJobNo">作业编号  1-1采购收货,3-1收货入库</param>
        /// <param name="scanType">扫描类型1.有条码 2.无条码</param>
        /// <param name="status">执行动作A.新增 S.過帳 Y.審核</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="Condition">多笔记录，包含两个属性一、项次//1.单号 2.日期  3.人员 4.部门 5.供货商6. 客户 7.料号 二、栏位值</param>
        /// <returns></returns>
        public DependencyObjectCollection GetFILPurchaseArrivalList(string programJobNo, string scanType, string status, string siteNo, DependencyObjectCollection condition) {
            #region 参数检查
            if (Maths.IsEmpty(programJobNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "program_job_no"));//‘入参【program_job_no】未传值’ 
            }
            #endregion
            QueryNode queryNode = GetFILPurchaseArrivalListNode(programJobNo, condition);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        

        #region 业务方法
        public QueryNode GetFILPurchaseArrivalListNode(string programJobNo, DependencyObjectCollection condition) {
            QueryConditionGroup group = (OOQL.AuthFilter("FIL_ARRIVAL", "FIL_ARRIVAL"))
                               & (OOQL.CreateProperty("FIL_ARRIVAL.STATUS") == OOQL.CreateConstants("2"));
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("FIL_ARRIVAL.DOC_NO", "FIL_ARRIVAL.DOC_DATE", new string[] { "1", "2", "5" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            QueryNode node = OOQL.Select(true,
                OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO","doc_no"),
                                         OOQL.CreateProperty("FIL_ARRIVAL.DOC_DATE","create_date"),
                                         OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_FULL_NAME", "customer_name"),
                                         OOQL.CreateConstants(programJobNo,"program_job_no"),
                                         OOQL.CreateConstants(string.Empty, "employee_name"))
                               .From("FIL_ARRIVAL", "FIL_ARRIVAL")
                               .LeftJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                               .On(OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID") == OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID"))
                               .LeftJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                               .On(OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO") == OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"))
                               .LeftJoin("SUPPLIER", "SUPPLIER")
                               .On(OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID") == OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID"))
                               .Where(group);
            return node;
        }
        #endregion
    }
}

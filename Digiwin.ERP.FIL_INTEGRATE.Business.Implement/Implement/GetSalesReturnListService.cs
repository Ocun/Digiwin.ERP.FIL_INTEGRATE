//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2017-02-13</createDate>
//<description>获取销退单通知服务</description>
//---------------------------------------------------------------- 
//20170328 modi by wangyq for P001-170327001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取销退单通知服务
    /// </summary>
    [ServiceClass(typeof(IGetSalesReturnListService))]
    [Description("获取销退单通知服务")]
    public class GetSalesReturnListService : ServiceComponent, IGetSalesReturnListService {
        #region 相关服务

        private IInfoEncodeContainer _infoEncodeContainer;
        /// <summary>
        /// 信息编码服务
        /// </summary>
        public IInfoEncodeContainer InfoEncodeSrv {
            get { return _infoEncodeContainer ?? (_infoEncodeContainer = GetService<IInfoEncodeContainer>()); }
        }

        #endregion


        #region IGetSalesReturnListService 接口成员
        /// <summary>
        /// 获取销退单通知
        /// </summary>
        /// <param name="programJobNo">作业编号  6.销退单</param>
        /// <param name="scanType">扫描类型  1.有条码 2.无条码</param>
        /// <param name="status">执行动作  A.新增  S.过帐</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetSalesReturnList(string programJobNo, string scanType, string status, string siteNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            #region 参数检查
            if (Maths.IsEmpty(programJobNo)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "program_job_no"));//‘入参【program_job_no】未传值’
            }
            if (Maths.IsEmpty(status)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "status"));//‘入参【status】未传值’
            }
            if (Maths.IsEmpty(siteNo)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "site_no"));//‘入参【site_no】未传值’
            }
            #endregion

            QueryNode queryNode = GetSalesReturnListQueryNode(siteNo, programJobNo, condition);//20170328 modi by wangyq for P001-170327001 添加参数condition
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }
        #endregion

        #region 业务员方法

        /// <summary>
        /// 获取销退单通知
        /// </summary>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private QueryNode GetSalesReturnListQueryNode(string siteNo, string programJobNo
            , DependencyObjectCollection condition//20170328 add by wangyq for P001-170327001
            ) {
            QueryNode queryNode =
                          OOQL.Select(true,
                                      OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_CODE", "main_organization"),
                                      OOQL.CreateProperty("SALES_RETURN.DOC_NO", "doc_no"),
                                      OOQL.CreateProperty("SALES_RETURN.DOC_DATE", "create_date"),
                                      OOQL.CreateProperty("CUSTOMER.CUSTOMER_NAME", "customer_name"),
                                      OOQL.CreateConstants(programJobNo, "program_job_no"),
                                      Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME"), OOQL.CreateConstants(string.Empty), "employee_name"))
                               .From("SALES_RETURN", "SALES_RETURN")
                               .InnerJoin("PLANT", "PLANT")
                               .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("SALES_RETURN.PLANT_ID"))
                               .InnerJoin("SALES_CENTER", "SALES_CENTER")
                               .On(OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_ID") == OOQL.CreateProperty("SALES_RETURN.Owner_Org.ROid"))
                               .InnerJoin("CUSTOMER", "CUSTOMER")
                               .On(OOQL.CreateProperty("CUSTOMER.CUSTOMER_ID") == OOQL.CreateProperty("SALES_RETURN.CUSTOMER_ID"))
                               .LeftJoin("EMPLOYEE", "EMPLOYEE")
                               .On(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("SALES_RETURN.Owner_Emp"))
                //20170328 add by wangyq for P001-170327001  ================begin==============
                               .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                               .On(OOQL.CreateProperty("SALES_RETURN.Owner_Dept") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"));
            //20170328 add by wangyq for P001-170327001  ================end==============
            //.Where(//20170328 mark by wangyq for P001-170327001
            QueryConditionGroup group =   //20170328 add by wangyq for P001-170327001
                  OOQL.CreateProperty("SALES_RETURN.CATEGORY") == OOQL.CreateConstants("26") &
                     OOQL.CreateProperty("SALES_RETURN.ApproveStatus") == OOQL.CreateConstants("Y") &
                     OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo) &
                     OOQL.CreateProperty("SALES_RETURN.RECEIPTED_STATUS").In(OOQL.CreateConstants("1"), OOQL.CreateConstants("2"));
            //);//20170328 mark by wangyq for P001-170327001

            //20170328 add by wangyq for P001-170327001  ================begin==============
            if (condition != null) {
                ConditionPropertyNameEntity conPropertyEntity = new ConditionPropertyNameEntity("SALES_RETURN.DOC_NO", "SALES_RETURN.DOC_DATE", new string[] { "1", "2", "3", "4", "6" });
                group = UtilsClass.CreateNewConditionByParameter(group, condition, conPropertyEntity);
            }
            queryNode = ((JoinOnNode)queryNode).Where(OOQL.AuthFilter("SALES_RETURN", "SALES_RETURN") & (group));
            //20170328 add by wangyq for P001-170327001  ================end==============
            return queryNode;
        }

        #endregion
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/14 13:49:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>获取工单工艺信息服务</description>
//20161208 modi by shenbao fro P001-161208001
//20161213 modi by shenbao for B001-161213006
//20170406 modi by wangyq for P001-170327001
//20170523 modi by shenbao for B001-170523018
//20170829 modi by shenbao for P001-170717001 A111273替换为A111585

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;
using System.Globalization;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [SingleGetCreator]
    [ServiceClass(typeof(IGetMoRoutingService))]
    [Description("获取工单工艺信息服务")]
    public sealed class GetMoRoutingService : ServiceComponent, IGetMoRoutingService {
        #region 相关服务

        private IInfoEncodeContainer _encodeSrv;
        /// <summary>
        /// 信息编码服务
        /// </summary>
        public IInfoEncodeContainer EncodeSrv {
            get {
                if (_encodeSrv == null)
                    _encodeSrv = this.GetService<IInfoEncodeContainer>();

                return _encodeSrv;
            }
        }

        #endregion

        #region IGetMoRoutingService 成员

        /// <summary>
        /// 获取工单工艺信息服务
        /// </summary>
        /// <param name="barcode_no">二维码</param>
        /// <param name="analysis_symbol">解析符号</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="report_type">报工类别</param>
        /// <returns></returns>
        public Hashtable GetMoRouting(string barcode_no, string analysis_symbol, string site_no, string report_type) {
            Hashtable result = new Hashtable();  //返回值
            DataTable dt = null;
            #region 参数检查
            if (Maths.IsEmpty(barcode_no)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "barcode_no" }));
            }
            if (Maths.IsEmpty(analysis_symbol)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "analysis_symbol" }));
            }
            if (Maths.IsEmpty(site_no)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "site_no" }));
            }
            #endregion

            //if (dt == null) { //参数检查没有问题时进入业务逻辑 //20161208 mark by shenbao fro P001-161208001 去掉多余的判断
            //查询逻辑
            string docNo = string.Empty, operationSeq = string.Empty;//单号、序号
            if (barcode_no.Contains(analysis_symbol)) {
                int index = barcode_no.LastIndexOf(analysis_symbol);
                docNo = barcode_no.Substring(0, index);
                operationSeq = barcode_no.Substring(index + 1);
            }

            int count = 0;
            if (docNo != string.Empty)
                count = QueryMO(docNo);
            if (count > 0) {
                dt = QueryMoRoutingInfo(docNo, operationSeq, report_type);
            } else {
                dt = QueryOtherInfo(barcode_no, site_no);
            }


            //组织返回逻辑
            if (dt != null && dt.Rows.Count > 0) {
                foreach (DataColumn column in dt.Columns)
                    result.Add(column.ColumnName, dt.Rows[0][column.ColumnName]);
            } else {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111585"));//20161213 add by shenbao for B001-161213006 条码中不包含条码分隔符或录入的信息不是正确的工单工序、机器、班组或人员信息！ //20170829 modi by shenbao for P001-170717001 A111273替换为A111585
            }

            return result;
        }

        #endregion

        #region 自定义方法

        private int QueryMO(string docNo) {
            QueryNode node = OOQL.Select("MO.MO_ID")
                .From("MO", "MO")
                .Where((OOQL.AuthFilter("MO", "MO"))
                    & (OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(docNo)));

            return this.GetService<IQueryService>().ExecuteDependencyObject(node).Count;
        }

        /// <summary>
        /// 获取工单工艺信息
        /// </summary>
        /// <param name="docNo">单号</param>
        /// <param name="operationSeq">工序</param>
        /// <returns></returns>
        private DataTable QueryMoRoutingInfo(string docNo, string operationSeq, string reportType) {
            #region 查询节点
            //reports_qty
            QueryProperty reportQty = null;
            if (reportType == "1")
                reportQty = OOQL.CreateProperty("MO_ROUTING_D.TO_ISSUE_QTY", "reports_qty");
            else if (reportType == "2")
                reportQty = OOQL.CreateProperty("MO_ROUTING_WIP.TO_CHECK_IN_QTY", "reports_qty");
            else if (reportType == "4")
                reportQty = OOQL.CreateProperty("MO_ROUTING_WIP.TO_CHECK_OUT_QTY", "reports_qty");
            else if (reportType == "3" || reportType == "5")
                reportQty = OOQL.CreateProperty("MO_ROUTING_WIP.TO_TRANSFER_QTY", "reports_qty");
            //20170406 add by wangyq for P001-170327001 ==========begin==============
            else if (reportType == "6") {
                reportQty = OOQL.CreateProperty("MO_ROUTING_WIP.TO_TRANSFER_QTY", "reports_qty");
            }
                //20170406 add by wangyq for P001-170327001 ==========end==============
            else
                reportQty = OOQL.CreateConstants(0, "reports_qty");

            //ok_qty
            QueryProperty okQty = null;
            if (reportType == "1")
                okQty = Formulas.IsNull(OOQL.CreateProperty("MO_ROUTING_WIP.ISSUED_QTY"), OOQL.CreateConstants(0M), "ok_qty");
            else if (reportType == "2")
                okQty = Formulas.IsNull(OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.CHECK_IN_QTY"), OOQL.CreateConstants(0M), "ok_qty");
            else if (reportType == "4")
                okQty = Formulas.IsNull(OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.CHECK_OUT_QTY"), OOQL.CreateConstants(0M), "ok_qty");
            else if (reportType == "3" || reportType == "5")
                okQty = Formulas.IsNull(OOQL.CreateProperty("WIP_TRANSFER_DOC_D.TRANSFER_QTY"), OOQL.CreateConstants(0M), "ok_qty");
            //20170406 add by wangyq for P001-170327001 ==========begin==============
            else if (reportType == "6") {
                okQty = Formulas.IsNull(OOQL.CreateProperty("WIP_TRANSFER_DOC_D.TRANSFER_QTY"), OOQL.CreateConstants(0M), "ok_qty");
            }
                //20170406 add by wangyq for P001-170327001 ==========end==============
            else
                okQty = OOQL.CreateConstants(0, "ok_qty");

            QueryNode node = OOQL.Select(
                    OOQL.CreateConstants(0, GeneralDBType.Int32, "code"),  //错误代号
                    OOQL.CreateConstants("", "sql_code"),  //回传代码
                    OOQL.CreateConstants("", "description"),  //错误信息
                    OOQL.CreateConstants("1", "return_type"),  //返回类型
                    OOQL.CreateProperty("MO.DOC_NO", "wo_no"),  //单号
                    OOQL.CreateConstants("0", "run_card_no"),
                    OOQL.CreateProperty("MO_ROUTING_D.OPERATION_SEQ", "seq"),  //工序
                    Formulas.IsNull(OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_CODE"), OOQL.CreateConstants(string.Empty), "workstation_no"),  //工作中心编号
                    Formulas.IsNull(OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_NAME"), OOQL.CreateConstants(string.Empty), "workstation_name"),  //工作中心名称
                    Formulas.IsNull(OOQL.CreateProperty("OPERATION.OPERATION_CODE"), OOQL.CreateConstants(string.Empty), "op_no"),  //工艺编号
                    Formulas.IsNull(OOQL.CreateProperty("OPERATION.OPERATION_NAME"), OOQL.CreateConstants(string.Empty), "op_name"),  //工艺名称
                    Formulas.IsNull(OOQL.CreateProperty("MO_ROUTING_D.OPERATION_SEQ"), OOQL.CreateConstants(string.Empty), "op_seq"),  //工序
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_CODE"), OOQL.CreateConstants(string.Empty), "item_no"),  //品号
                    Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty), "item_name"),  //品名
                    reportQty,
                    okQty,
                    OOQL.CreateProperty("MO_ROUTING_D.POSITION_FLAG"),
                    OOQL.CreateProperty("MO.ACTUAL_START_DATETIME"),
                    OOQL.CreateProperty("MO_ROUTING_WIP.FIRST_CHECKIN_TIME"),
                    OOQL.CreateConstants(0, "work_hours"),
                    OOQL.CreateConstants(0, "component_seq"),
                    OOQL.CreateConstants("", "component_itmo_no"),
                    OOQL.CreateConstants(0, "qpa_molecular"),
                    OOQL.CreateConstants(1, "qpa_denominator"),
                    OOQL.CreateConstants("", "unit_no")
                )
                .From("MO_ROUTING", "MO_ROUTING")
                .InnerJoin("MO")
                .On(OOQL.CreateProperty("MO_ROUTING.MO_ID") == OOQL.CreateProperty("MO.MO_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("MO.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .InnerJoin("MO_ROUTING.MO_ROUTING_D", "MO_ROUTING_D")
                .On(OOQL.CreateProperty("MO_ROUTING.MO_ROUTING_ID") == OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_ID"))
                .LeftJoin("MO_ROUTING.MO_ROUTING_D.MO_ROUTING_WORK_CENTER", "MO_ROUTING_WORK_CENTER")
                .On(OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_D_ID") == OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.MO_ROUTING_D_ID")
                    & OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.MAIN_STATION") == OOQL.CreateConstants(true))
                .LeftJoin("MO_ROUTING.MO_ROUTING_D.MO_ROUTING_WIP", "MO_ROUTING_WIP")
                .On(OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_D_ID") == OOQL.CreateProperty("MO_ROUTING_WIP.MO_ROUTING_D_ID")
                    & OOQL.CreateProperty("MO_ROUTING_WIP.SOURCE_ID.ROid") == OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.SOURCE_ID.ROid"))
                .InnerJoin("OPERATION")
                .On(OOQL.CreateProperty("MO_ROUTING_D.OPERATION_ID") == OOQL.CreateProperty("OPERATION.OPERATION_ID"))
                .LeftJoin("WORK_CENTER")
                .On(OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.SOURCE_ID.ROid") == OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_ID"))
                .LeftJoin(OOQL.Select(
                        OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.SOURCE_ID.ROid", "SOURCE_ID_ROid"),
                        OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.MO_ROUTING_D_ID"),
                        Formulas.Sum(Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants(0), new CaseItem[]{
                            new CaseItem(OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.ACTION_TYPE")==OOQL.CreateConstants("2")
                                ,OOQL.CreateProperty("QTY"))
                        })
                        , OOQL.CreateConstants(0)), "CHECK_IN_QTY"),
                        Formulas.Sum(Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants(0), new CaseItem[]{
                            new CaseItem(OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.ACTION_TYPE")==OOQL.CreateConstants("3")
                                ,OOQL.CreateProperty("QTY"))
                        })
                        , OOQL.CreateConstants(0)), "CHECK_OUT_QTY")
                    )
                    .From("SHOP_FLOOR_ACTION_LOG")
                    .Where(OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.ApproveStatus") == OOQL.CreateConstants("Y"))
                    .GroupBy(OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.SOURCE_ID.ROid")
                        , OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.MO_ROUTING_D_ID"))
                , "SHOP_FLOOR_ACTION_LOG")
                .On(OOQL.CreateProperty("MO_ROUTING_WIP.SOURCE_ID.ROid") == OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.SOURCE_ID_ROid")
                    & OOQL.CreateProperty("MO_ROUTING_WIP.MO_ROUTING_D_ID") == OOQL.CreateProperty("SHOP_FLOOR_ACTION_LOG.MO_ROUTING_D_ID"))
                .LeftJoin(OOQL.Select(
                        OOQL.CreateProperty("WIP_TRANSFER_DOC.SOURCE_ID.ROid", "SOURCE_ID_ROid"),
                        OOQL.CreateProperty("WIP_TRANSFER_DOC_D.FROM_MO_ROUTING_D_ID"),
                        Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("WIP_TRANSFER_DOC_D.QTY")), OOQL.CreateConstants(0), "TRANSFER_QTY")
                    )
                    .From("WIP_TRANSFER_DOC.WIP_TRANSFER_DOC_D", "WIP_TRANSFER_DOC_D")
                    .InnerJoin("WIP_TRANSFER_DOC")
                    .On(OOQL.CreateProperty("WIP_TRANSFER_DOC_D.WIP_TRANSFER_DOC_ID") == OOQL.CreateProperty("WIP_TRANSFER_DOC.WIP_TRANSFER_DOC_ID"))
                    .Where(OOQL.CreateProperty("WIP_TRANSFER_DOC.ApproveStatus") == OOQL.CreateConstants("Y"))
                    .GroupBy(OOQL.CreateProperty("WIP_TRANSFER_DOC.SOURCE_ID.ROid")
                        , OOQL.CreateProperty("WIP_TRANSFER_DOC_D.FROM_MO_ROUTING_D_ID"))
                , "WIP_TRANSFER_DOC_D")
                .On(OOQL.CreateProperty("MO_ROUTING_WIP.SOURCE_ID.ROid") == OOQL.CreateProperty("WIP_TRANSFER_DOC_D.SOURCE_ID_ROid")
                    & OOQL.CreateProperty("MO_ROUTING_WIP.MO_ROUTING_D_ID") == OOQL.CreateProperty("WIP_TRANSFER_DOC_D.FROM_MO_ROUTING_D_ID"))
                .Where((OOQL.AuthFilter("MO_ROUTING", "MO_ROUTING"))
                    & (OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(docNo)
                    & OOQL.CreateProperty("MO_ROUTING_D.OPERATION_SEQ") == OOQL.CreateConstants(operationSeq))
                    );
            #endregion

            DataTable dt = this.GetService<IQueryService>().Execute(node);

            //处理工时
            //由于平台的Formulas.DateDiff不支持分钟，且用秒的话会导致数据溢出，
            //顾先将时间查出来，重新再计算
            foreach (DataRow dr in dt.Rows) {
                long workHours = 0L;
                if (reportType == "1") {
                    //这里由于调用TotalMinutes.ToInt32()之后数据被转成了0，估计是ToInt32的扩展函数有bug
                    //所以就用微软的转换，且这里不会出现异常，因为前面的一定是个double
                    workHours = Convert.ToInt32((DateTime.Now - dr["ACTUAL_START_DATETIME"].ToDate()).TotalMinutes, CultureInfo.CurrentCulture);
                } else {
                    workHours = Convert.ToInt32((DateTime.Now - dr["FIRST_CHECKIN_TIME"].ToDate()).TotalMinutes, CultureInfo.CurrentCulture);
                }
                dr["work_hours"] = workHours;
            }

            if (dt != null && dt.Rows.Count > 0) {
                if (reportType == "1" && (new string[] { "1", "2" }.Contains(dt.Rows[0]["POSITION_FLAG"].ToStringExtension())
                        || dt.Rows[0]["reports_qty"].ToDecimal() <= 0m)) {
                    throw new BusinessRuleException(EncodeSrv.GetMessage("A111237"));
                } else if (reportType == "2" && dt.Rows[0]["reports_qty"].ToDecimal() <= 0m) {
                    throw new BusinessRuleException(EncodeSrv.GetMessage("A111238"));
                } else if (reportType == "4" && dt.Rows[0]["reports_qty"].ToDecimal() <= 0m) {
                    throw new BusinessRuleException(EncodeSrv.GetMessage("A111239"));
                } else if (new string[] { "3", "5" }.Contains(reportType) && (dt.Rows[0]["reports_qty"].ToDecimal() <= 0m
                    || new string[] { "1", "3" }.Contains(dt.Rows[0]["POSITION_FLAG"].ToStringExtension()))//20170406 add by wangyq for P001-170327001  //20170523 modi by shenbao for B001-170523018 POSITION_FLAG==>1,3
                    ) {
                    throw new BusinessRuleException(EncodeSrv.GetMessage("A111240"));
                } else if (reportType == "6" && (dt.Rows[0]["reports_qty"].ToDecimal() <= 0m//20170406 add by wangyq for P001-170327001 ==========begin==============
                    || new string[] { "0", "2" }.Contains(dt.Rows[0]["POSITION_FLAG"].ToStringExtension()))) {  //20170523 modi by shenbao for B001-170523018 POSITION_FLAG==>0,2
                    throw new BusinessRuleException(EncodeSrv.GetMessage("A111412"));
                }//20170406 add by wangyq for P001-170327001 ==========end==============
            }
            dt.Columns.Remove("POSITION_FLAG");
            dt.Columns.Remove("ACTUAL_START_DATETIME");
            dt.Columns.Remove("FIRST_CHECKIN_TIME");

            return dt;
        }

        /// <summary>
        /// 查询机器信息或者班组信息或者人员信息
        /// </summary>
        /// <param name="barcode_no"></param>
        /// <param name="site_no"></param>
        /// <returns></returns>
        private DataTable QueryOtherInfo(string barcode_no, string site_no) {
            QueryNode node = OOQL.Select("MACHINE.MACHINE_ID")
                .From("MACHINE", "MACHINE")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("MACHINE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .Where((OOQL.AuthFilter("MACHINE", "MACHINE"))
                    & (OOQL.CreateProperty("MACHINE.MACHINE_CODE") == OOQL.CreateConstants(barcode_no)
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    ));

            IQueryService qrySrv = this.GetService<IQueryService>();
            DataTable dtMathine = qrySrv.Execute(node);

            node = OOQL.Select("MACHINE_TEAM.MACHINE_TEAM_ID")
                .From("MACHINE_TEAM", "MACHINE_TEAM")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("MACHINE_TEAM.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .Where((OOQL.AuthFilter("MACHINE_TEAM", "MACHINE_TEAM"))
                    & (OOQL.CreateProperty("MACHINE_TEAM.TEAM_CODE") == OOQL.CreateConstants(barcode_no)
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    ));

            DataTable dtMathineItem = qrySrv.Execute(node);

            //返回数据的键值对
            //该字典记录了除了公共字段code、sql_code、description、return_type以外的其他字段的值
            Dictionary<string, object> otherValue = null;
            if ((dtMathine != null && dtMathine.Rows.Count > 0)
                || (dtMathineItem != null && dtMathineItem.Rows.Count > 0)) {//二维码为机器
                DataTable dt = CreateEmptyTable("return_type", "machine_no");
                otherValue = new Dictionary<string, object>();
                otherValue.Add("return_type", "2");
                otherValue.Add("machine_no", barcode_no);
                SetValueToTable(dt, 0, "", "", otherValue);
                return dt;
            } else {
                node = OOQL.Select("WORK_TEAM.WORK_TEAM_ID")
                .From("WORK_TEAM", "WORK_TEAM")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("WORK_TEAM.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .Where((OOQL.AuthFilter("WORK_TEAM", "WORK_TEAM"))
                    & (OOQL.CreateProperty("WORK_TEAM.WORK_TEAM_CODE") == OOQL.CreateConstants(barcode_no)
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    ));
                DataTable dtWorkItem = qrySrv.Execute(node);
                if (dtWorkItem != null && dtWorkItem.Rows.Count > 0) {  //二维码为班组
                    DataTable dt = CreateEmptyTable("return_type", "shift_no");
                    otherValue = new Dictionary<string, object>();
                    otherValue.Add("return_type", "3");
                    otherValue.Add("shift_no", barcode_no);
                    SetValueToTable(dt, 0, "", "", otherValue);
                    return dt;
                } else {
                    node = OOQL.Select("EMPLOYEE.EMPLOYEE_ID", "EMPLOYEE.EMPLOYEE_CODE", "EMPLOYEE.EMPLOYEE_NAME")
                    .From("EMPLOYEE", "EMPLOYEE")
                    .Where((OOQL.AuthFilter("EMPLOYEE", "EMPLOYEE"))
                        & (OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE") == OOQL.CreateConstants(barcode_no)
                        ));
                    DataTable dtEmployee = qrySrv.Execute(node);
                    if (dtEmployee != null && dtEmployee.Rows.Count > 0) { //二维码为人员
                        DataTable dt = CreateEmptyTable("return_type", "employee_no", "employee_name");
                        otherValue = new Dictionary<string, object>();
                        otherValue.Add("return_type", "4");
                        otherValue.Add("employee_no", dtEmployee.Rows[0]["EMPLOYEE_CODE"].ToStringExtension());
                        otherValue.Add("employee_name", dtEmployee.Rows[0]["EMPLOYEE_NAME"].ToStringExtension());
                        SetValueToTable(dt, 0, "", "", otherValue);
                        return dt;
                    }
                }

            }

            return null;
        }

        /// <summary>
        /// 创建空记录
        /// </summary>
        /// <param name="otherColumns">除了公共字段code、sql_code、description以外的其他字段</param>
        /// <returns></returns>
        private DataTable CreateEmptyTable(params string[] otherColumns) {
            //公共字段
            List<string> columnNames = new List<string>{
                "code",
                "sql_code",
                "description"
            };
            List<Type> types = new List<Type>{
                typeof(int),
                typeof(string),
                typeof(string)
            };

            if (otherColumns != null && otherColumns.Length > 0) {
                columnNames.AddRange(otherColumns);
                for (int i = 0; i < otherColumns.Length; i++)
                    types.Add(typeof(string));
            }

            DataTable dt = UtilsClass.CreateDataTable("result", columnNames.ToArray(), types.ToArray());

            return dt;
        }

        /// <summary>
        /// SQL异常时 返回对应的错误信息
        /// </summary>
        /// <param name="dt">结果集合</param>
        /// <param name="code">错误编码</param>
        /// <param name="sqlCode">sql异常</param>
        /// <param name="description">错误描述</param>
        private void SetValueToTable(DataTable dt, int code, string sqlCode, string description) {
            this.SetValueToTable(dt, code, sqlCode, description, null);
        }

        /// <summary>
        /// SQL异常时 返回对应的错误信息
        /// </summary>
        /// <param name="dt">结果集合</param>
        /// <param name="code">错误编码</param>
        /// <param name="sqlCode">sql异常</param>
        /// <param name="description">错误描述</param>
        /// <param name="returnType">返回类型</param>
        /// <param name="otherValues">除了公共字段code、sql_code、description、return_type以外的其他字段的值</param>
        private void SetValueToTable(DataTable dt, int code, string sqlCode, string description, Dictionary<string, object> otherValues) {
            DataRow dr = dt.NewRow();
            dr["code"] = code;
            dr["sql_code"] = sqlCode;
            dr["description"] = description;
            if (otherValues != null) {
                foreach (string key in otherValues.Keys)
                    dr[key] = otherValues[key];
            }

            dt.Rows.Add(dr);
        }

        /// <summary>
        /// 获取OOQL执行结果
        /// 如果有异常，则为SQL类型异常
        /// </summary>
        /// <param name="node">查询节点</param>
        /// <param name="isSqlException">是否为sql类型异常</param>
        /// <returns></returns>
        private DataTable GetOOQLExcuteResult(QueryNode node, ref bool isSqlException) {
            DataTable dt = null;
            try {
                dt = this.GetService<IQueryService>().Execute(node);
            } catch (Exception ex) {
                dt = CreateEmptyTable();
                DataRow dr = dt.NewRow();
                dr["code"] = -1;
                dr["sql_code"] = ex.Message;
                dr["description"] = string.Empty;
                dt.Rows.Add(dr);

                isSqlException = true;
            }

            return dt;
        }

        #endregion
    }
}

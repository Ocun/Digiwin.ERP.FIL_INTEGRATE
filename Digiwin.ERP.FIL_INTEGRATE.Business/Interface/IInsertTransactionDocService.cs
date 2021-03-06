﻿//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/3/27 15:49:22</CreateDate>
//<IssueNO>P001-170327001</IssueNO>
//<Description>生成出入库单接口</Description>
//---------------------------------------------------------------- 
using System;
using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///  生成出入库单接口
    /// </summary>
    [TypeKeyOnly]
    [Description("生成出入库单接口")]
    public interface IInsertTransactionDocService {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="employee_no">扫描人员</param>
        /// <param name="scan_type">扫描类型1.有箱条码 2.无箱条码</param>
        /// <param name="report_datetime">上传时间</param>
        /// <param name="picking_department_no">部门</param>
        /// <param name="recommended_operations">建议执行作业</param>
        /// <param name="recommended_function">A.新增  S.过帐</param>
        /// <param name="scan_doc_no">扫描单号</param>
        /// <param name="collScan"></param>
        DependencyObjectCollection InertTransactionDoc(string employee_no, string scan_type, DateTime report_datetime,
           string picking_department_no, string recommended_operations, string recommended_function,
           string scan_doc_no, DependencyObjectCollection scanColl);
    }
}

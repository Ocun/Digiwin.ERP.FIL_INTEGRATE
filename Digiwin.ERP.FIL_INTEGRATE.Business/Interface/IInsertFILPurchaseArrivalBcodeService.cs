//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-3-27</createDate>
//<IssueNo>P001-170316001</IssueNo>
//<description>依送货单条码生成到货单服务接口</description>
//----------------------------------------------------------------
using System;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("依送货单条码生成到货单服务接口")]
    public interface IInsertFILPurchaseArrivalBcodeService {
        /// <summary>
        /// 依送货单条码生成到货单服务接口
        /// </summary>
        /// <param name="employee_no">扫描人员</param>
        /// <param name="scan_type">扫描类型1.有箱条码 2.无箱条码</param>
        /// <param name="report_datetime">上传时间</param>
        /// <param name="picking_department_no">领料部门</param>
        /// <param name="recommended_operations">建议执行作业</param>
        /// <param name="recommended_function">A.新增  S.过帐</param>
        /// <param name="scan_doc_no">扫描单号</param>
        /// <param name="scanColl"></param>
        DependencyObjectCollection InsertFILPurchaseArrivalBcode(string employee_no, string scan_type, DateTime report_datetime, string picking_department_no,
            string recommended_operations, string recommended_function, string scan_doc_no, DependencyObjectCollection scanColl);
    }
}

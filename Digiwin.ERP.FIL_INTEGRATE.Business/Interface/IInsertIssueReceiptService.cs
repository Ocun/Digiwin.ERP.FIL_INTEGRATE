//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/10 10:19:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>产生领退料单服务接口</description>
//----------------------------------------------------------------  

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("产生领退料单服务接口")]
    public interface IInsertIssueReceiptService {
        /// <summary>
        ///  产生领退料单
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型 1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collection">接口传入的领料单单身数据集合</param>
        DependencyObjectCollection InsertIssueReceipt(string employeeNo, string scanType, DateTime reportDatetime, string pickingDepartmentNo
            , string recommendedOperations, string recommendedFunction, string scanDocNo, DependencyObjectCollection collection);
    }
}

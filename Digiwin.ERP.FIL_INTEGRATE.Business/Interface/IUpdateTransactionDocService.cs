//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>zhangcn</author>
//<createDate>2016/11/15 13:59:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>更新出入库单服务 接口</description>
//----------------------------------------------------------------  

using System;
using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("更新出入库单服务 接口")]
    public interface IUpdateTransactionDocService {
        /// <summary>
        /// 更新库存交易单单身
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collScan">参数集合</param>
        DependencyObjectCollection DoUpdateTransactionDoc(string employeeNo, string scanType, DateTime reportDatetime,
            string pickingDepartmentNo, string recommendedOperations, string recommendedFunction,
            string scanDocNo, DependencyObjectCollection collScan);
    }
}

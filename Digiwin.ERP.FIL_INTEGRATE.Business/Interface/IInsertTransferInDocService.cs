//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-17</createDate>
//<description>生成拨入单 接口</description>
//---------------------------------------------------------------- 

using System;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///生成拨入单 接口
    /// </summary>
    [TypeKeyOnly]
    [Description("生成拨入单 接口")]
    public interface IInsertTransferInDocService {
        /// <summary>
        /// 插入拨入单
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collScan">参数集合</param>
        /// <returns>新生成的单号DocNo</returns>
        DependencyObjectCollection DoInsertTransferInDoc(string employeeNo, string scanType, DateTime reportDatetime, 
            string pickingDepartmentNo,string recommendedOperations,string recommendedFunction,
            string scanDocNo,DependencyObjectCollection collScan);
    }
}

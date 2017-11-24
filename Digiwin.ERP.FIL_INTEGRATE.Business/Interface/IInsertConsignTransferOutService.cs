//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>liwei1</author>
//<createDate>2017/07/20</createDate>
//<IssueNo>P001-170717001</IssueNo>
//<description>产生寄售调拨单服务接口</description>

using System;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    public interface IInsertConsignTransferOutService {
        /// <summary>
        ///  产生寄售调拨单信息
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型 1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collection">接口传入的领料单单身数据集合</param>
        DependencyObjectCollection InsertConsignTransferOut(string employeeNo, string scanType, DateTime reportDatetime, string pickingDepartmentNo
            , string recommendedOperations, string recommendedFunction, string scanDocNo, DependencyObjectCollection collection);
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/12/23 13:49:37</createDate>
//<IssueNo>P001-161215001</IssueNo>
//<description>取得出货指示服务接口</description>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common;
using System.Collections;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    public interface IInstructionsService {
        /// <summary>
        /// 取得出货指示
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="doc_no">单据编号</param>
        /// <param name="site_no">工厂</param>
        /// <param name="warehouse_no">库位编号</param>
        /// <returns></returns>
        Hashtable GetInstructions(string program_job_no, string status, DependencyObjectCollection param_master, string site_no, string warehouse_no);
    }
}
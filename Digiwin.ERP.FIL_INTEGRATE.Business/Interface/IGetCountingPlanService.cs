//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>shenbao</author>
//<createDate>2017/02/03 09:19:37</createDate>
//<IssueNo>P001-170124001</IssueNo>
//<description>获取盘点计划服务接口</description>
// modi by 08628 for P001-171023001

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common.Torridity;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    public interface IGetCountingPlanService {
        /// <summary>
        /// 获取盘点计划
        /// </summary>
        /// <param name="counting_type">盘点类型</param>
        /// <param name="warehouse_no">仓库</param>
        /// <param name="counting_no">盘点计划编号</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="barcode_no">条码编号</param>
        /// <returns></returns>
        Hashtable GetCountingPlan(string counting_type, string warehouse_no, string counting_no, string site_no,string barcode_no);
    }
}
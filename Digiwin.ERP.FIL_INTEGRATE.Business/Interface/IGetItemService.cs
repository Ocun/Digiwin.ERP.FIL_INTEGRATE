//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/10 17:19:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>获取品号信息服务接口</description>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common;
using System.Data;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    public interface IGetItemService {
        /// <summary>
        /// 根据传入的条码，获取品号信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号集合(可能是单个品号,也可能是一个集合)</param>
        /// <param name="mainOrganization">主营组织</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="mainOrganizationType">组织类型</param>
        /// <returns></returns>
        DependencyObjectCollection GetItem(string programJobNo, DependencyObjectCollection itemId, object mainOrganization, string siteNo
            , string mainOrganizationType);
    }
}
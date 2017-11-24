//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-04-28</createDate>
//<description>获取企业信息服务</description>
//----------------------------------------------------------------

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetCompanyService))]
    [Description("获取企业信息服务")]
    public class GetCompanyService : ServiceComponent, IGetCompanyService {
        /// <summary>
        /// 获取企业信息
        /// </summary>
        /// <returns></returns>
        public Hashtable GetCompany() {
            DependencyObjectType type = new DependencyObjectType("enterprise_detail");
            type.RegisterSimpleProperty("enterprise_no", typeof(string));
            type.RegisterSimpleProperty("enterprise_name", typeof(string));
            DependencyObjectCollection enterpriseDetail = new DependencyObjectCollection(type);
            const string company = "99";
            DependencyObject entity = enterpriseDetail.AddNew();
            entity["enterprise_no"] = company;
            entity["enterprise_name"] = company;
            return new Hashtable { { "enterprise_detail", enterpriseDetail } };
        }
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-15</createDate>
//<description>营运中心信息获取接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 营运中心信息获取接口
    /// </summary>
    [TypeKeyOnly]
    [Description("营运中心信息获取接口")]
    public interface IGetPlantService{
        /// <summary>
        /// 根据传入的工厂编号，查询工厂的其他相关信息
        /// </summary>
        /// <param name="site_no">门店编号</param>
        /// <param name="enterprise_no">公司别</param>
        /// <returns></returns>
        Hashtable GetPlant(string site_no, string enterprise_no);
    }
}

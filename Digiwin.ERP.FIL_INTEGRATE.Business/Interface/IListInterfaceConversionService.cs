//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-16</createDate>
//<description>上传转换接口服务</description>
//---------------------------------------------------------------- 
//20170328 modi by liwei1 for P001-170327001	增加入参过滤条件

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {

    /// <summary>
    /// 上传转换接口服务
    /// </summary>
    [TypeKeyOnly]
    [Description("通知转换接口服务")]
    public interface IListInterfaceConversionService {

        /// <summary>
        /// 通知接口转换服务
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="scan_type">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="site_no">工厂</param>
        /// <returns></returns>
        Hashtable listInterfaceConversion(string program_job_no, string status, string scan_type, string site_no);

        /// <summary>
        /// 通知接口转换服务
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="scan_type">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="site_no">工厂</param>
        /// <returns></returns>
        Hashtable listInterfaceConversion(string program_job_no, string status, string scan_type, string site_no, DependencyObjectCollection condition);//20170328 add by liwei1 for P001-170327001
    }
}

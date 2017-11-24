//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-08</createDate>
//<description>转换接口服务接口</description>
//---------------------------------------------------------------- 

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {

    /// <summary>
    /// 转换接口服务接口
    /// </summary>
    [TypeKeyOnly]
    [Description("转换接口服务接口")]
    public interface IInterfaceConversionService {
        /// <summary>
        /// 接口转换服务
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="doc_no">单据编号</param>
        /// <param name="site_no">工厂</param>
        /// <param name="scan_type">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="seq">项次</param>
        /// <returns></returns>
        Hashtable InterfaceConversion(string program_job_no, string status, string site_no, string scan_type, DependencyObjectCollection param_master);//参数名的名称必须与Json中的参数名称一致。
        
    }
}

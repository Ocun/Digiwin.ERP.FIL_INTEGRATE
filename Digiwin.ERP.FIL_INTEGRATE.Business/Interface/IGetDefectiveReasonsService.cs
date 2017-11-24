//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-07-14</createDate>
//<description>获取不良原因接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取不良原因接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取不良原因接口")]
    public interface IGetDefectiveReasonsService {
        
        /// <summary>
        /// 根据传入的不良原因编号，获取相应的不良原因信息
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="site_no">工厂</param>
        /// <param name="reason_code">理由码</param>
        /// <returns></returns>
        Hashtable GetDefectiveReasons(string program_job_no, string status, string site_no, string reason_code);
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-12</createDate>
//<description>删除送货单接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 删除送货单接口
    /// </summary>
    [TypeKeyOnly]
    [Description("删除送货单接口")]
    public interface IDeleteFilArrivalService {
        /// <summary>
        /// 根据传入的单号信息删除送货单
        /// </summary>
        /// <param name="delivery_no">送货单号</param>
        /// <returns></returns>
        void DeleteFilArrival(string delivery_no);
    }
}

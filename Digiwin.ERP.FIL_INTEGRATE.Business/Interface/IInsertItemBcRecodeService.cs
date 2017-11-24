//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-04-12</createDate>
//<description>料件条码创建接口</description>
//---------------------------------------------------------------- 
using System.ComponentModel;
using Digiwin.Common;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 料件条码创建接口
    /// </summary>
    [TypeKeyOnly]
    [Description("料件条码创建接口")]
    public interface IInsertItemBcRecodeService {

        /// <summary>
        /// 根据传入的送货单单号信息获取根据明细产生条码基础资料档
        /// </summary>
        /// <param name="delivery_no">送货单号</param>
        /// <returns></returns>
        void InsertItemBcRecode(string delivery_no);
    }
}

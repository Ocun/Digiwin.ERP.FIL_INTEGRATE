//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-05-01</createDate>
//<description>生成到货检验单接口</description>
//---------------------------------------------------------------- 

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    ///生成到货检验单接口
    /// </summary>
    [TypeKeyOnly]
    [Description("生成到货检验单接口")]
    public interface IInsertPoArrivalInspectionService {

        /// <summary>
        /// 根据传入的信息，生成到货检验单信息
        /// </summary>
        /// <param name="delivery_no">送货单</param>
        /// <param name="supplier_no">供货商</param>
        /// <param name="supplier_name">供货商名称</param>
        /// <param name="receipt_no">收货单</param>
        /// <param name="receipt_list"></param>
        /// <returns></returns>
        Hashtable InsertPoArrivalInspection(string delivery_no, string supplier_no, string supplier_name, string receipt_no, DependencyObjectCollection receipt_list);
    }
}
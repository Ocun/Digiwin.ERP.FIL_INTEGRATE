//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-11</createDate>
//<description>获取条码服务接口</description>
//---------------------------------------------------------------- 
//20161222 modi by liwei1 for P001-161215001
using System.Collections.Generic;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    /// <summary>
    /// 获取条码服务接口
    /// </summary>
    [TypeKeyOnly]
    [Description("获取条码服务接口")]
    public interface IGetBarcodeService {

        /// <summary>
        /// 获取条码服务
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="scanType">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="status">执行动作</param>
        /// <returns></returns>
        DependencyObjectCollection GetBarcode(string[] docNo, string scanType, List<object> itemId, List<object> itemFeatureId, string siteNo, string programJobNo, string status);//20161222 add by liwei1 for P001-161215001
        //DependencyObjectCollection GetBarcode(string docNo, string scanType, List<object> itemId, List<object> itemFeatureId, string siteNo, string programJobNo, string status);//20161222 mark by liwei1 for P001-161215001
    }
}

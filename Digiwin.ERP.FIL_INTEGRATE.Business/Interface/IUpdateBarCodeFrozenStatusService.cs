//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-3-27</createDate>
//<IssueNo>P001-170316001</IssueNo>
//<description>更新条码冻结状态服务接口</description>
//----------------------------------------------------------------
using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("更新条码冻结状态服务 接口")]
    public interface IUpdateBarCodeFrozenStatusService {
        /// <summary>
        /// 更新条码冻结状态服务接口
        /// </summary>
        /// <param name="barcodeNo">条码</param>
        /// <param name="status">凍結狀態：N.未冻结，Y.冻结</param>
        /// <param name="siteNo">营运据点</param>
        void UpdateBarCodeFrozenStatus(string barcode_no, string status, string site_no);
    }
}

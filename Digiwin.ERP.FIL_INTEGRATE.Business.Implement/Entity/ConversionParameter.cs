//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-09</createDate>
//<description>接口转换服务参数对象</description>
//----------------------------------------------------------------
//20161216 modi by liwei1 for P001-161215001 逻辑调整
using System.ComponentModel;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    [Description("接口转换服务参数对象")]
    class ConversionParameter {
        /// <summary>
        /// 作业编号
        /// </summary>
        public string ProgramJobNo { get; set; }

        /// <summary>
        /// 状态：A.新增  S.过帐
        /// </summary>
        public string Status { get; set; }

        /// <summary>
        /// 单据编号
        /// </summary>
        //public string DocNo { get; set; }//20161216 mark by liwei1 for P001-161215001
        public string[] DocNo { get; set; }//20161216 add by liwei1 for P001-161215001

        /// <summary>
        /// 工厂
        /// </summary>
        public string SiteNo { get; set; }

        /// <summary>
        /// 扫描类型：1.箱条码 2.单据条码 3.品号
        /// </summary>
        public string ScanType { get; set; }

        /// <summary>
        /// 项次
        /// </summary>
        public string Seq { get; set; }
    }
}

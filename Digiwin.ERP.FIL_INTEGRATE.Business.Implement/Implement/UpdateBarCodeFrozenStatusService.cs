//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-3-27</createDate>
//<IssueNo>P001-170316001</IssueNo>
//<description>更新条码冻结状态服务实现</description>
//----------------------------------------------------------------
//20170413 modi by wangrm for P001-170412001 修改传参名称
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IUpdateBarCodeFrozenStatusService))]
    [Description("更新条码冻结状态服务 实现")]
    public class UpdateBarCodeFrozenStatusService : ServiceComponent, IUpdateBarCodeFrozenStatusService {
        #region IUpdateBarCodeFrozenStatusService 成员
        /// <summary>
        /// 更新条码冻结状态服务实现
        /// </summary>
        /// <param name="barcodeNo">条码</param>
        /// <param name="status">凍結狀態：N.未冻结，Y.冻结</param>
        /// <param name="siteNo">营运据点</param>
        public void UpdateBarCodeFrozenStatus(string barcode_no, string status, string site_no) {//20170413 modi by wangrm for P001-170412001 OLD：barcodeNo
            #region 参数检查
            if (Maths.IsEmpty(barcode_no)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "barcode_no"));//‘入参【barcode_no】未传值’ 
            }
            if (Maths.IsEmpty(status)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));//‘入参【status】未传值’
            }
            #endregion

            QueryNode updateNode = OOQL.Update("BC_RECORD",new SetItem[]{
                    new SetItem(OOQL.CreateProperty("BC_RECORD.FROZEN_STATUS"),OOQL.CreateConstants(status))
                })
                .Where(OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcode_no));
            this.GetService<IQueryService>().ExecuteNoQueryWithManageProperties(updateNode);

        }

        #endregion
    }
}

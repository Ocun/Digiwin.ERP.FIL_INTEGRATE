//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-22</createDate>
//<description>下载单据转换服务</description>
//----------------------------------------------------------------
//20160110 modi by shenbao for P001-170110001 处理采购收货单据等
//20170111 modi by shenbao for P001-170111001
//20170215 modi by liwei1 for P001-170203001
//20170328 modi by liwei1 for P001-170316001 增加5S和6S的情况\增加送货单条码采购收入及收货入库
//20170725 modi by shenbao for P001-170717001 厂内智能物流新需求
//20170731 modi by liwei1 for P001-170717001 增加5-1寄售调拨
//20170801 modi by shenbao for P001-170717001
//20170925 add by wangyq for P001-170717001

using System;
using System.Collections;
using System.ComponentModel;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IInterfaceConversionNewService))]
    [Description("下载单据转换服务")]
    public class InterfaceConversionNewService : ServiceComponent, IInterfaceConversionNewService {

        /// <summary>
        /// 接口转换服务
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="site_no">工厂</param>
        /// <param name="scan_type">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="seq">项次</param>
        /// <param name="doc_no">单据编号</param>
        /// <returns></returns>
        public Hashtable InterfaceConversionNew(string program_job_no, string status, string site_no, string scan_type, DependencyObjectCollection param_master) {//参数名的名称必须与Json中的参数名称一致。
            try {
                //实例化接口转换服务参数
                ConversionParameter conversionPara = new ConversionParameter();
                if (param_master.Count > 0) {
                    conversionPara.DocNo = param_master.Select(x => x["doc_no"].ToStringExtension()).ToArray();
                }
                conversionPara.ProgramJobNo = program_job_no;
                conversionPara.Status = status;
                conversionPara.SiteNo = site_no;
                conversionPara.ScanType = scan_type;

                //定义barcode_detail
                DependencyObjectType barCodeDt = new DependencyObjectType("source_doc_detail");
                DependencyObject barCodeObj = new DependencyObject(barCodeDt);
                DependencyObjectCollection sourceDocDetail = new DependencyObjectCollection(barCodeDt);
                switch (program_job_no) {
                    case "2"://2.采购入库
                    case "2-1":  //20170111 add by shenbao for P001-170111001
                        IGetPurchaseArrivalService getPurchaseArrivalSrv = this.GetService<IGetPurchaseArrivalService>(this.TypeKey);
                        sourceDocDetail = getPurchaseArrivalSrv.GetPurchaseArrival(conversionPara.ProgramJobNo, "2", "A",
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    //20160110 add by shenbao for P001-170110001 ===begin===
                    case "1":
                    case "3":
                        IGetPurchaseOrderService getPurchaseOrderSrv = this.GetService<IGetPurchaseOrderService>(this.TypeKey);
                        sourceDocDetail = getPurchaseOrderSrv.GetPurchaseOrder(conversionPara.ProgramJobNo, "A", "2",
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    //20160110 add by shenbao for P001-170110001 ===end===

                    #region 20170328 add by liwei1 for P001-170316001  //【入参program_job_no】=”1-1.采购收货” OR “3-1.收货入库”
                    case "1-1":
                    case "3-1":
                        IGetFILPurchaseArrivalBcodeService getFILPurchaseArrivalBcodeSrv = this.GetService<IGetFILPurchaseArrivalBcodeService>(this.TypeKey);
                        sourceDocDetail = getFILPurchaseArrivalBcodeSrv.GetFILPurchaseArrivalBcode(conversionPara.ProgramJobNo, "1", "A", conversionPara.DocNo, conversionPara.SiteNo);
                        break;
                    #endregion

                    //20170215 add by liwei1 for P001-170203001===begin===、
                    case "4"://4.采购退货
                        IGetPurchaseReturnService getPurchaseReturnSrv = this.GetService<IGetPurchaseReturnService>(this.TypeKey);
                        sourceDocDetail = getPurchaseReturnSrv.GetPurchaseReturn(conversionPara.ProgramJobNo, "2", "A",
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    case "6"://6.销退单
                        IGetSalesReturnService getSalesReturnSrv = this.GetService<IGetSalesReturnService>(this.TypeKey);
                        sourceDocDetail = getSalesReturnSrv.GetSalesReturn(conversionPara.ProgramJobNo, "2", conversionPara.Status, //20170328 modi by liwei1 for P001-170316001 old:执行动作(status) = A.新增
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    //20170215 add by liwei1 for P001-170203001===end===

                    case "5"://5.销售出货 
                        IGetSalesDeliveryService getSalesDeliverySrv = this.GetService<IGetSalesDeliveryService>(this.TypeKey);
                        sourceDocDetail = getSalesDeliverySrv.GetSalesDelivery(conversionPara.ProgramJobNo, conversionPara.Status, "2", //20170328 modi by liwei1 for P001-170316001 old:执行动作(status) = A.新增
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;

                    //20170731 add by liwei1 for P001-170717001===begin===
                    case "5-1"://5.寄售调拨 
                        IGetSalesOrderService getSalesOrderService = GetService<IGetSalesOrderService>(TypeKey);
                        sourceDocDetail = getSalesOrderService.GetSalesOrder(conversionPara.ProgramJobNo, conversionPara.Status, "2",
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    //20170731 add by liwei1 for P001-170717001===end===

                    case "7":
                    case "8"://7.工单发料 or 8.工单退料
                    case "7-5"://20170925 add by wangyq for P001-170717001
                        IGetIssueReceiptMoService getIssueReceiptMoSrv = this.GetService<IGetIssueReceiptMoService>(this.TypeKey);
                        sourceDocDetail = getIssueReceiptMoSrv.GetIssueReceiptMo(conversionPara.ProgramJobNo, "2", conversionPara.Status,
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    case "7-3"://7-3.领料申请单
                        IGetIssueReceiptReqService getIssueReceiptReqSrv = this.GetService<IGetIssueReceiptReqService>(this.TypeKey);
                        sourceDocDetail = getIssueReceiptReqSrv.GetIssueReceiptReq(conversionPara.ProgramJobNo, "2", conversionPara.Status,
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    case "9-2"://9-2.生产入库单
                        IGetMOReceiptReqService getMOReceiptReqSrv = this.GetService<IGetMOReceiptReqService>(this.TypeKey);
                        sourceDocDetail = getMOReceiptReqSrv.GetMOReceiptReq(conversionPara.ProgramJobNo, "2", conversionPara.Status,
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    //20170725 add by shenbao for P001-170717001 ===begin===
                    case "9-1"://9-1.入库申请
                    case "9-3"://9-3.生产入库工單  //20170801 add by shenbao for P001-170717001
                        IGetMOProductMoService getMOProductMoSrv = this.GetService<IGetMOProductMoService>(this.TypeKey);
                        sourceDocDetail = getMOProductMoSrv.GetMOProduct(conversionPara.ProgramJobNo, "2", conversionPara.Status,
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    //20170725 add by shenbao for P001-170717001 ===end===
                    case "11"://11.杂项发料
                    case "12"://12.杂项收料
                        IGetTransactionDocService getTransactionDocSrv = this.GetService<IGetTransactionDocService>(this.TypeKey);
                        sourceDocDetail = getTransactionDocSrv.GetTransactionDoc(conversionPara.ProgramJobNo, "2", "S",
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    case "13-3"://13-3.拨入单 
                        IGetTransferDocService getTransferDocSrv = this.GetService<IGetTransferDocService>(this.TypeKey);
                        sourceDocDetail = getTransferDocSrv.GetTransferDoc(conversionPara.ProgramJobNo, "2", conversionPara.Status,
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    case "13-5"://13-5.调拨单
                        IGetTransferReqService getTransferReqSrv = this.GetService<IGetTransferReqService>(this.TypeKey);
                        sourceDocDetail = getTransferReqSrv.GetTransferReq(conversionPara.ProgramJobNo, "2", conversionPara.Status,
                            conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                        break;
                    //20170925 add by wangyq for P001-170717001  =============begin===============
                    case "5-2":
                        IGetSalesIssueService getSalesIssueSrv = this.GetService<IGetSalesIssueService>(this.TypeKey);
                        sourceDocDetail = getSalesIssueSrv.GetSalesIssue(conversionPara.ProgramJobNo, "2", conversionPara.Status
                            , conversionPara.DocNo, conversionPara.SiteNo, Maths.GuidDefaultValue().ToString());
                        break;
                    //20170925 add by wangyq for P001-170717001  =============end===============
                }

                //组合返回结果
                Hashtable result = new Hashtable();
                //添加单据下载数据
                result.Add("source_doc_detail", sourceDocDetail);

                return result;
            } catch (Exception) {
                throw;
            }
        }
    }
}

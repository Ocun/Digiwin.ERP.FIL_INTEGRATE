//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-16</createDate>
//<description>通知转换服务</description>
//----------------------------------------------------------------
//20161228 mido by liwei1 for P001-161215001
//20170111 modi by shenbao for P001-170111001
//20170215 modi by liwei1 for P001-170203001
//20170328 modi by liwei1 for P001-170327001	增加入参过滤条件
//20170328 modi by liwei1 for P001-170316001 增加送货单条码采购收入及收货入库
//20170731 modi by liwei1 for P001-170717001 增加5-1寄售调拨
//20170801 modi by shenbao for P001-170717001
//20170925 modi by wangyq for P001-170717001

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IListInterfaceConversionService))]
    [Description("通知转换服务")]
    public class ListInterfaceConversionService : ServiceComponent, IListInterfaceConversionService {

        #region IListInterfaceConversionService 成员

        public Hashtable listInterfaceConversion(string program_job_no, string status, string scan_type, string site_no) {
            //20170328 add by liwei1 for P001-170327001 ---begin---
            //旧的服务调用新的服务，最后一个参数传null
            return this.listInterfaceConversion(program_job_no, status, scan_type, site_no, null);
        }

        public Hashtable listInterfaceConversion(string program_job_no, string status, string scan_type, string site_no, DependencyObjectCollection condition) {
            //20170328 add by liwei1 for P001-170327001 ---end---
            try {

                DependencyObjectType barCodeDt = new DependencyObjectType("sales_notice");
                DependencyObject barCodeObj = new DependencyObject(barCodeDt);
                DependencyObjectCollection salesNotice = new DependencyObjectCollection(barCodeDt);
                //20161228 add by liwei1 for P001-161215001 用switch代替Ifelse,增加7-3.领料申请单、13-5.调拨单
                switch (program_job_no) {
                    case "2"://2.收货入库
                    case "2-1":  //20170111 add by shenbao for P001-170111001
                        IGetPurchaseArrivalListService getPurchaseArrivalListSrv = this.GetService<IGetPurchaseArrivalListService>(this.TypeKey);
                        salesNotice = getPurchaseArrivalListSrv.GetPurchaseArrival(program_job_no, "1", "A", site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;

                    #region 20170328 modi by liwei1 for P001-170316001  //【入参program_job_no】=”1-1.采购收货” OR “3-1.收货入库”
                    case "1-1":
                    case "3-1":
                        IGetFILPurchaseArrivalListService getFILPurchaseArrivalListSrv = this.GetService<IGetFILPurchaseArrivalListService>(this.TypeKey);
                        salesNotice = getFILPurchaseArrivalListSrv.GetFILPurchaseArrivalList(program_job_no, "1", "A", site_no, condition);
                        break;
                    #endregion

                    //20170215 add by liwei1 for P001-170203001===begin===
                    case "4"://4.采购退货
                        IGetPurchaseReturnListService getPurchaseReturnListSrv = this.GetService<IGetPurchaseReturnListService>(this.TypeKey);
                        salesNotice = getPurchaseReturnListSrv.GetPurchaseReturnList(program_job_no, "1", "A", site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    case "6"://6.销退单
                        IGetSalesReturnListService getSalesReturnListSrv = this.GetService<IGetSalesReturnListService>(this.TypeKey);
                        salesNotice = getSalesReturnListSrv.GetSalesReturnList(program_job_no, "1", "A", site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    //20170215 add by liwei1 for P001-170203001===end===
                    case "5"://5.销货出库
                        IGetSalesDeliveryListService getSalesDeliveryListSrv = this.GetService<IGetSalesDeliveryListService>(this.TypeKey);
                        salesNotice = getSalesDeliveryListSrv.GetSalesDeliveryList(program_job_no, "1", "A", site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    //20170731 add by liwei1 for P001-170717001===begin===
                    case "5-1"://5.寄售调拨
                        IGetSalesOrderListService getSalesOrderListService = GetService<IGetSalesOrderListService>(TypeKey);
                        salesNotice = getSalesOrderListService.GetSalesOrderList(program_job_no, "1", "A", site_no, condition);
                        break;
                    //20170731 add by liwei1 for P001-170717001===end===
                    case "7":
                    case "8"://7.工单发料 OR 8.工单退料
                    case "7-5"://20170925 add by wangyq for P001-170717001
                        IGetIssueReceiptListService getIssueReceiptListSrv = this.GetService<IGetIssueReceiptListService>(this.TypeKey);
                        salesNotice = getIssueReceiptListSrv.GetIssueReceipt(program_job_no, "1", status, site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    case "7-3"://7-3.领料申请单
                        IGetIssueReceiptReqListService getIssueReceiptReqListSrv = this.GetService<IGetIssueReceiptReqListService>(this.TypeKey);
                        salesNotice = getIssueReceiptReqListSrv.GetIssueReceiptReqList(program_job_no, "1", status, site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    case "9-2"://9-2.生产入库单
                        IGetMOReceiptReqListService getMOReceiptReqListSrv = this.GetService<IGetMOReceiptReqListService>(this.TypeKey);
                        salesNotice = getMOReceiptReqListSrv.GetMOReceiptReqList(program_job_no, "1", status, site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    //20170801 add by shenbao for P001-170717001 ===begin===
                    case "9-3"://9-3.生产入库工单
                        IGetMOListService getMOListSrv = this.GetService<IGetMOListService>(this.TypeKey);
                        salesNotice = getMOListSrv.GetMOList(program_job_no, "1", status, site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    //20170801 add by shenbao for P001-170717001 ===end===
                    case "11":
                    case "12"://11.杂项发料 OR 12.杂项收料
                        IGetTransactionDocListService getTransactionDocListSrv = this.GetService<IGetTransactionDocListService>(this.TypeKey);
                        salesNotice = getTransactionDocListSrv.GetTransactionDoc(program_job_no, "1", status, site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    case "13-3"://13-3.拨入单
                        IGetTransferDocListService getTransferDocListSrv = this.GetService<IGetTransferDocListService>(this.TypeKey);
                        salesNotice = getTransferDocListSrv.GetTransferDoc(program_job_no, "1", status, site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    case "13-5"://13-5.调拨单
                        IGetTransferReqListService getTransferReqListSrv = this.GetService<IGetTransferReqListService>(this.TypeKey);
                        salesNotice = getTransferReqListSrv.GetTransferReqList(program_job_no, "1", status, site_no
                            , condition//20170328 add by liwei1 for P001-170327001
                            );
                        break;
                    //20170925 add by wangyq for P001-170717001  =============begin===============
                    case "5-2":
                        IGetSalesIssueListService getSalesIssueListSrv = this.GetService<IGetSalesIssueListService>(this.TypeKey);
                        salesNotice = getSalesIssueListSrv.GetSalesIssueList(program_job_no, scan_type, status, site_no, condition);
                        break;
                    //20170925 add by wangyq for P001-170717001  =============end===============
                }
                //20161228 add by liwei1 for P001-161215001
                #region 20161228 mark by liwei1 for P001-161215001 用switch代替Ifelse,性能好点
                //if (program_job_no == "2") {//2.收货入库
                //    IGetPurchaseArrivalListService getPurchaseArrivalListSrv = this.GetService<IGetPurchaseArrivalListService>(this.TypeKey);
                //    salesNotice = getPurchaseArrivalListSrv.GetPurchaseArrival(program_job_no, "1", "A", site_no);
                //} else if (program_job_no == "5") {//5.销货出库
                //    IGetSalesDeliveryListService getSalesDeliveryListSrv = this.GetService<IGetSalesDeliveryListService>(this.TypeKey);
                //    salesNotice = getSalesDeliveryListSrv.GetSalesDeliveryList(program_job_no, "1", "A", site_no);
                //} else if (program_job_no == "7" || program_job_no == "8") {//7.工单发料 OR 8.工单退料
                //    IGetIssueReceiptListService getIssueReceiptListSrv = this.GetService<IGetIssueReceiptListService>(this.TypeKey);
                //    salesNotice = getIssueReceiptListSrv.GetIssueReceipt(program_job_no, "1", status, site_no);
                //} else if (program_job_no == "11" || program_job_no == "12") {//11.杂项发料 OR 12.杂项收料
                //    IGetTransactionDocListService getTransactionDocListSrv = this.GetService<IGetTransactionDocListService>(this.TypeKey);
                //    salesNotice = getTransactionDocListSrv.GetTransactionDoc(program_job_no, "1", status, site_no);
                //} else if (program_job_no == "13-3") {//13-3.拨入单
                //    IGetTransferDocListService getTransferDocListSrv = this.GetService<IGetTransferDocListService>(this.TypeKey);
                //    salesNotice = getTransferDocListSrv.GetTransferDoc(program_job_no, "1", status, site_no);
                //}
                #endregion
                //组织返回结果
                Hashtable result = new Hashtable();
                result.Add("sales_notice", salesNotice);
                return result;
            } catch (Exception) {
                throw;
            }
        }

        #endregion
    }
}

//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-16</createDate>
//<description>上传转换接口服务实现</description>
//----------------------------------------------------------------
//20161207 modi by liwei1 for B001-161206022 Old:ToString()
//20170103 modi by liwei1 for P001-161215001 新增领料申请单，调拨申请单、生产入库申请单
//20170111 modi by shenbao for P001-170111001
//20170215 modi by liwei1 for P001-170203001
//20170327 modi by wangyq for P001-170327001
//20170726 modi by shenbao for P001-170717001 厂内智能物流新需求
//20170801 modi by shenbao for P001-170717001
//20170731 modi by liwei1 for P001-170717001 增加5-1寄售调拨

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IInsertInterfaceConversionService))]
    [Description("上传转换服务实现")]
    public class InsertInterfaceConversionService : ServiceComponent, IInsertInterfaceConversionService {

        #region IInsertInterfaceConversionService 成员
        /// <summary>
        /// 上传接口转换服务
        /// </summary>
        /// <param name="employee_no">扫描人员</param>
        /// <param name="scan_type">扫描类型1.有箱条码 2.无箱条码</param>
        /// <param name="report_datetime">上传时间</param>
        /// <param name="picking_department_no">领料部门</param>
        /// <param name="recommended_operations">建议执行作业</param>
        /// <param name="recommended_function">A.新增  S.过帐</param>
        /// <param name="scan_doc_no">扫描单号</param>
        /// <param name="scan">单据数据</param>
        /// <returns>单据编号</returns>
        public Hashtable InsertInterfaceConversion(string employee_no, string scan_type, string report_datetime, string picking_department_no,
            string recommended_operations, string recommended_function, string scan_doc_no, DependencyObjectCollection scan) {
            try {
                DependencyObjectCollection docNoCollection = null;
                if (recommended_operations == "1" || recommended_operations == "3") {//1.采购收货 OR 3.收货入库
                    IInsertPurchaseArrivalService insertPurchaseArrivalSrv = this.GetService<IInsertPurchaseArrivalService>(this.TypeKey);
                    docNoCollection = insertPurchaseArrivalSrv.InsertPurchaseArrival(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                } else if (recommended_operations == "1-1" || recommended_operations == "3-1") {//1.采购收货 OR 3.收货入库
                    IInsertFILPurchaseArrivalBcodeService insertFILPurchaseArrivalBcodeSrv = this.GetService<IInsertFILPurchaseArrivalBcodeService>(this.TypeKey);
                    docNoCollection = insertFILPurchaseArrivalBcodeSrv.InsertFILPurchaseArrivalBcode(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                } else if (recommended_operations == "2" || recommended_operations == "2-1") {//2.采购入库  //20170111 modi by shenbao for P001-170111001 添加2-1
                    IInsertPurchaseReceiptService insertPurchaseReceiptSrv = this.GetService<IInsertPurchaseReceiptService>(this.TypeKey);
                    docNoCollection = insertPurchaseReceiptSrv.DoInsertPurchaseReceipt(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);

                    //20170215 add by liwei1 for P001-170203001 ===begin===
                } else if (recommended_operations == "4") {//4.采购退货
                    IInsertPurchaseIssueService insertPurchaseIssueSrv = this.GetService<IInsertPurchaseIssueService>(this.TypeKey);
                    docNoCollection = insertPurchaseIssueSrv.DoInsertPurchaseIssue(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                } else if (recommended_operations == "6") {//6.销退单
                    IInsertSalesReturnReceiptService insertSalesReturnReceiptSrv = this.GetService<IInsertSalesReturnReceiptService>(this.TypeKey);
                    docNoCollection = insertSalesReturnReceiptSrv.DoInsertSalesReturnReceipt(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                    //20170215 add by liwei1 for P001-170203001 ===end===

                    //20170731 add by liwei1 for P001-170717001===begin===
                } else if (recommended_operations == "5-1") {//5.寄售调拨
                    IInsertConsignTransferOutService insertConsignTransferOutSrv = this.GetService<IInsertConsignTransferOutService>(this.TypeKey);
                    docNoCollection = insertConsignTransferOutSrv.InsertConsignTransferOut(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                    //20170731 add by liwei1 for P001-170717001===end===

                } else if (recommended_operations == "5") {//5.销售出货
                    IInsertSalesIssueService insertSalesIssueSrv = this.GetService<IInsertSalesIssueService>(this.TypeKey);
                    docNoCollection = insertSalesIssueSrv.InsertSalesIssue(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                    //20170905 add by wangyq for P001-170717001  =================begin===================
                } else if (recommended_operations == "5-2" && recommended_function == "S") {//销售拣货核对+过账
                    IUpdateSalesIssueService updateSalesIssueService = this.GetService<IUpdateSalesIssueService>(this.TypeKey);
                    docNoCollection = updateSalesIssueService.UpdateSalesIssue(employee_no, scan_type, report_datetime.ToDate(),
                       picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                    //20170905 add by wangyq for P001-170717001  =================end===================
                    //（入参recommended_operations】=(7.工单发料 OR 8.工单退料)and 【入参recommended_function】=A.新增）OR 【入参recommended_operations】= 7-3.领料申请单
                } else if (((recommended_operations == "7" || recommended_operations == "8") && recommended_function == "A"
                      ) || recommended_operations == "7-3") {//20170103 add by liwei1 for P001-161215001
                    IInsertIssueReceiptService insertIssueReceiptSrv = this.GetService<IInsertIssueReceiptService>(this.TypeKey);
                    docNoCollection = insertIssueReceiptSrv.InsertIssueReceipt(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                } else if ((recommended_operations == "7" || recommended_operations == "8" || recommended_operations == "7-5") && recommended_function == "S") {//【入参recommended_operations】=(7.工单发料 OR 8.工单退料)and 【入参recommended_function】=S.过账//20170905 modi by wangyq for P001-170717001 添加7-5
                    IUpdateIssueReceiptService updateIssueReceiptSrv = this.GetService<IUpdateIssueReceiptService>(this.TypeKey);
                    docNoCollection = updateIssueReceiptSrv.UpdateIssueReceipt(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                } else if (recommended_operations == "9"
                    || recommended_operations == "9-2"//20170103 add by liwei1 for P001-161215001
                    || recommended_operations == "9-3"//20170801 add by shenbao for P001-170717001
                    ) {//5.销售出货
                    IInsertMOReceiptService insertMoProductSrv = this.GetService<IInsertMOReceiptService>(this.TypeKey);
                    docNoCollection = insertMoProductSrv.InsertMOReceipt(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                } else if (recommended_operations == "9-1" && recommended_function == "A"//20170726 add by shenbao for P001-170717001
                    ) {//9-1.入库申请
                    IInsertMOReceiptReqService insertMoReceiptReqSrv = this.GetService<IInsertMOReceiptReqService>(this.TypeKey);
                    docNoCollection = insertMoReceiptReqSrv.InsertMOReceiptReq(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                } else if (recommended_operations == "11" || recommended_operations == "12") {//11.杂项发料 OR 12.杂项收料
                    if (recommended_function == "S") {//过账 20170327 add by wangyq for P001-170327001
                        IUpdateTransactionDocService updateTransactionDocSrv = this.GetService<IUpdateTransactionDocService>(this.TypeKey);
                        docNoCollection = updateTransactionDocSrv.DoUpdateTransactionDoc(employee_no, scan_type, report_datetime.ToDate(),
                            picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                    } else if (recommended_function == "A") {//新增 20170327 add by wangyq for P001-170327001  ======================begin======================
                        IInsertTransactionDocService inertTransactionDocService = this.GetServiceForThisTypeKey<IInsertTransactionDocService>();
                        docNoCollection = inertTransactionDocService.InertTransactionDoc(employee_no, scan_type, report_datetime.ToDate().Date, picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                    }
                    //20170327 add by wangyq for P001-170327001  ======================end======================
                } else if (recommended_operations == "13-1" || recommended_operations == "13-2"
                    || recommended_operations == "13-5"//20170103 add by liwei1 for P001-161215001
                    ) {//13-1.调拨单 OR 13-2.调出单
                    IInsertTransferDocService insertTransferDocSrv = this.GetService<IInsertTransferDocService>(this.TypeKey);
                    docNoCollection = insertTransferDocSrv.DoInsertTransferDoc(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                } else if (recommended_operations == "13-3") {//13-3.拨入单
                    IInsertTransferInDocService insertTransferInDocSrv = this.GetService<IInsertTransferInDocService>(this.TypeKey);
                    docNoCollection = insertTransferInDocSrv.DoInsertTransferInDoc(employee_no, scan_type, report_datetime.ToDate(),
                        picking_department_no, recommended_operations, recommended_function, scan_doc_no, scan);
                }

                string docNo = string.Empty;
                if (docNoCollection != null && docNoCollection.Count > 0) {
                    foreach (var item in docNoCollection) {
                        if (Maths.IsEmpty(docNo)) {
                            docNo = item["doc_no"].ToStringExtension();//20161207 modi by liwei1 for B001-161206022 Old:ToString()
                        } else {
                            docNo += "/" + item["doc_no"].ToStringExtension();//20161207 modi by liwei1 for B001-161206022 Old:ToString()
                        }
                    }
                }
                //组织返回结果
                Hashtable result = new Hashtable();
                result.Add("doc_no", docNo);
                return result;

            } catch (Exception) {
                throw;
            }
        }

        #endregion
    }
}

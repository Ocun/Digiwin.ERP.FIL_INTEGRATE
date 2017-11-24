//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-08</createDate>
//<description>转换接口服务接口 实现</description>
//----------------------------------------------------------------
//20161216 modi by liwei1 for P001-161215001 逻辑调整
//20170724 modi by shenbao for P001-170717001 厂内智能物流新需求
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IInterfaceConversionService))]
    [Description("转换接口服务接口实现")]
    public class InterfaceConversionService : ServiceComponent, IInterfaceConversionService {

        #region 全局变量
        /// <summary>
        /// 品号异步返回值
        /// </summary>
        AsyncResultObject _ItemAsyncObject;

        /// <summary>
        /// 条码异步返回值
        /// </summary>
        AsyncResultObject _BarcodeAsyncObject;

        /// <summary>
        /// 单据异步返回值
        /// </summary>
        AsyncResultObject _SourceDocObject;
        #endregion

        /// <summary>
        /// 异步下载委托
        /// </summary>
        /// <returns></returns>
        delegate void DownloadDelegate(ConversionParameter conversionPara);

        #region IInterfaceConversionService 成员
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
        public Hashtable InterfaceConversion(string program_job_no, string status, string site_no, string scan_type, DependencyObjectCollection param_master) {//参数名的名称必须与Json中的参数名称一致。
            Hashtable result = new Hashtable();
            //实例化全局变量
            InstantiatedVariables();
            //实例化接口转换服务参数
            ConversionParameter conversionPara = new ConversionParameter();
            //暂时只做一笔处理，后期可能要考虑多笔
            if (param_master.Count > 0) {
                conversionPara.DocNo = param_master.Select(x => x["doc_no"].ToStringExtension()).ToArray();//20161216 add by liwei1 for P001-161215001
            }//20161216 add by liwei1 for P001-161215001

            conversionPara.ProgramJobNo = program_job_no;
            conversionPara.Status = status;
            //conversionPara.DocNo = param_master[0]["doc_no"].ToStringExtension();//暂时只做一笔处理，后期可能要考虑多笔      //20161216 mark by liwei1 for P001-161215001
            conversionPara.SiteNo = site_no;
            conversionPara.ScanType = scan_type;
            //conversionPara.Seq = param_master[0]["seq"].ToStringExtension();//暂时只做一笔处理，后期可能要考虑多笔    //20161216 mark by liwei1 for P001-161215001

            //IF  【入参program_job_no】=1.采购收货 OR 3.收货入库 THEN
            if (program_job_no == "1" || program_job_no == "3") {
                //1. CALL下载采购单服务
                DownloadDelegate sourceDocDelegate = new DownloadDelegate(GetPurchaseOrderData);
                sourceDocDelegate.BeginInvoke(conversionPara, SourceDocProxyCallback, null);

                //2. CALL下载品号服务
                DownloadDelegate itemDelegate = new DownloadDelegate(GetPurchaseOrderItemData);
                itemDelegate.BeginInvoke(conversionPara, ItemProxyCallback, null);

                //3. CALL下载条码服务
                DownloadDelegate barcodeDelegate = new DownloadDelegate(GetPurchaseOrderBarcodeData);
                barcodeDelegate.BeginInvoke(conversionPara, BarcodeProxyCallback, null);
            } else if (program_job_no == "2") {//【入参program_job_no】=2.采购入库
                //1. CALL下载到货单服务
                DownloadDelegate sourceDocDelegate = new DownloadDelegate(GetPurchaseArrivalData);
                sourceDocDelegate.BeginInvoke(conversionPara, SourceDocProxyCallback, null);
                //2. CALL下载到货单品号服务
                DownloadDelegate itemDelegate = new DownloadDelegate(GetPurchaseArrlivalItemData);
                itemDelegate.BeginInvoke(conversionPara, ItemProxyCallback, null);

                ////3. CALL下载到货单条码服务
                DownloadDelegate barcodeDelegate = new DownloadDelegate(GetPurchaseArrlivalBarcodeData);
                barcodeDelegate.BeginInvoke(conversionPara, BarcodeProxyCallback, null);
            } else if (program_job_no == "5") { //【入参program_job_no】=5.销售出货
                //1. CALL下载销货单服务
                DownloadDelegate sourceDocDelegate = new DownloadDelegate(GetSalesDeliveryData);
                sourceDocDelegate.BeginInvoke(conversionPara, SourceDocProxyCallback, null);
                //2. CALL下载销货单品号服务
                DownloadDelegate itemDelegate = new DownloadDelegate(GetSalesDeliveryItemData);
                itemDelegate.BeginInvoke(conversionPara, ItemProxyCallback, null);

                //3. CALL下载销货单条码服务
                DownloadDelegate barcodeDelegate = new DownloadDelegate(GetSalesDeliveryBarcodeData);
                barcodeDelegate.BeginInvoke(conversionPara, BarcodeProxyCallback, null);
            } else if (program_job_no == "7" || program_job_no == "8") { //【入参program_job_no】=7.工单发料 OR 8.工单退料
                //1. CALL下载领退料单服务
                DownloadDelegate sourceDocDelegate = new DownloadDelegate(GetIssueReceiptMoData);
                sourceDocDelegate.BeginInvoke(conversionPara, SourceDocProxyCallback, null);
                //2. CALL下载领退料单品号服务
                DownloadDelegate itemDelegate = new DownloadDelegate(GetIssueReceiptMoItemData);
                itemDelegate.BeginInvoke(conversionPara, ItemProxyCallback, null);

                //3. CALL下载领退料单条码服务
                DownloadDelegate barcodeDelegate = new DownloadDelegate(GetIssueReceiptMoBarcodeData);
                barcodeDelegate.BeginInvoke(conversionPara, BarcodeProxyCallback, null);
            } else if (program_job_no == "9") { //【入参program_job_no】=9.完工入库
                //1. CALL下载入库工单服务
                DownloadDelegate sourceDocDelegate = new DownloadDelegate(GetMoProductMoData);
                sourceDocDelegate.BeginInvoke(conversionPara, SourceDocProxyCallback, null);

                //2. CALL下载入库工单品号服务
                DownloadDelegate itemDelegate = new DownloadDelegate(GetMoProductMoItemData);
                itemDelegate.BeginInvoke(conversionPara, ItemProxyCallback, null);

                //3. CALL下载入库工单条码服务
                DownloadDelegate barcodeDelegate = new DownloadDelegate(GetMoProductMoBarcodeData);
                barcodeDelegate.BeginInvoke(conversionPara, BarcodeProxyCallback, null);
            } else if (program_job_no == "11" || program_job_no == "12") { //【入参program_job_no】=11.杂项发料 OR 12.杂项收料
                //1. CALL获取杂发、杂收服务
                DownloadDelegate sourceDocDelegate = new DownloadDelegate(GetTransactionDocData);
                sourceDocDelegate.BeginInvoke(conversionPara, SourceDocProxyCallback, null);
                //2. CALL下载杂发、杂收品号服务
                DownloadDelegate itemDelegate = new DownloadDelegate(GetTransactionDocItemData);
                itemDelegate.BeginInvoke(conversionPara, ItemProxyCallback, null);

                //3. CALL下载杂发、杂收条码服务
                DownloadDelegate barcodeDelegate = new DownloadDelegate(GetTransactionDocBarcodeData);
                barcodeDelegate.BeginInvoke(conversionPara, BarcodeProxyCallback, null);
            } else if (program_job_no == "13-1" || program_job_no == "13-2") { //【入参program_job_no】=13-1.调拨单 OR 13-2.调出单
                //此情况不存在下载单据故应该给异步处理“下单据实例”默认处理完成。否则后面死循环中无法跳出。
                _SourceDocObject.IsCompleted = true;

                //2. CALL下载杂发、杂收品号服务
                DownloadDelegate itemDelegate = new DownloadDelegate(GetTransferDocItemData);
                itemDelegate.BeginInvoke(conversionPara, ItemProxyCallback, null);

                //3. CALL下载杂发、杂收条码服务
                DownloadDelegate barcodeDelegate = new DownloadDelegate(GetTransferDocBarcodeData);
                barcodeDelegate.BeginInvoke(conversionPara, BarcodeProxyCallback, null);
            } else if (program_job_no == "13-3") { //【入参program_job_no】=13-3.拨入单
                //1. CALL获取杂发、杂收服务
                DownloadDelegate sourceDocDelegate = new DownloadDelegate(GetTransferInDocData);
                sourceDocDelegate.BeginInvoke(conversionPara, SourceDocProxyCallback, null);
                //2. CALL下载杂发、杂收品号服务
                DownloadDelegate itemDelegate = new DownloadDelegate(GetTransferInDocItemData);
                itemDelegate.BeginInvoke(conversionPara, ItemProxyCallback, null);

                //3. CALL下载杂发、杂收条码服务
                DownloadDelegate barcodeDelegate = new DownloadDelegate(GetTransferInDocBarcodeData);
                barcodeDelegate.BeginInvoke(conversionPara, BarcodeProxyCallback, null);
            }

            bool isOk = true;//异步处理全部完成变更为false，跳出循环
            do {
                //判断异步处理是否全部完成
                if (_ItemAsyncObject.IsCompleted
                    && _BarcodeAsyncObject.IsCompleted
                    &&
                    _SourceDocObject.IsCompleted
                    ) {
                    //结果循环处理
                    isOk = false;

                    //判断异步处理中是否存在异常，不存在异常添加对应数据至Hashtable中，作为结果返回
                    if (_SourceDocObject.IsHappenException
                        || _ItemAsyncObject.IsHappenException
                        || _BarcodeAsyncObject.IsHappenException
                        ) {
                        //结果处理
                        isOk = false;
                        //组合所有异常信息
                        string msg = string.Empty;
                        if (_SourceDocObject.Exception != null) {
                            msg += _SourceDocObject.Exception.Message;
                        }
                        if (_ItemAsyncObject.Exception != null) {
                            msg += _ItemAsyncObject.Exception.Message;
                        }
                        if (_BarcodeAsyncObject.Exception != null) {
                            msg += _BarcodeAsyncObject.Exception.Message;
                        }
                        throw new Exception(msg);//返回异常信息
                    } else {
                        //如果存在返回结果为null，此处返回空集合
                        if (_SourceDocObject.Result == null) {
                            //定义barcode_detail
                            DependencyObjectType barCodeDt = new DependencyObjectType("source_doc_detail");
                            DependencyObject barCodeObj = new DependencyObject(barCodeDt);
                            DependencyObjectCollection barCodeDetail = new DependencyObjectCollection(barCodeDt);
                            _SourceDocObject.Result = barCodeDetail;
                        }
                        if (_ItemAsyncObject.Result == null) {
                            //定义barcode_detail
                            DependencyObjectType barCodeDt = new DependencyObjectType("item_detail");
                            DependencyObject barCodeObj = new DependencyObject(barCodeDt);
                            DependencyObjectCollection barCodeDetail = new DependencyObjectCollection(barCodeDt);
                            _ItemAsyncObject.Result = barCodeDetail;
                        }
                        if (_BarcodeAsyncObject.Result == null) {
                            //定义barcode_detail
                            DependencyObjectType barCodeDt = new DependencyObjectType("barcode_detail");
                            DependencyObject barCodeObj = new DependencyObject(barCodeDt);
                            DependencyObjectCollection barCodeDetail = new DependencyObjectCollection(barCodeDt);
                            _BarcodeAsyncObject.Result = barCodeDetail;
                        }
                        //添加单据下载数据
                        result.Add("source_doc_detail", _SourceDocObject.Result);
                        //添加品号下载数据
                        result.Add("item_detail", _ItemAsyncObject.Result);
                        //添加条码下载数据
                        result.Add("barcode_detail", _BarcodeAsyncObject.Result);
                    }
                    break;
                }
            } while (isOk);
            //} //20161216 mark by liwei1 for P001-161215001

            return result;
        }
        #endregion

        /// <summary>
        /// 实例化全局变量
        /// </summary>
        private void InstantiatedVariables() {
            _ItemAsyncObject = new AsyncResultObject();//实例化品号异步返回值对象
            _BarcodeAsyncObject = new AsyncResultObject();//实例化条码异步返回值对象
            _SourceDocObject = new AsyncResultObject();//实例化单据异步返回值对象

        }

        #region 异步回调处理方法
        /// <summary>
        /// 下载品号回调处理
        /// </summary>
        /// <param name="result"></param>
        private void ItemProxyCallback(IAsyncResult result) {
            AsyncResult async = (AsyncResult)result;
            DownloadDelegate del = (DownloadDelegate)async.AsyncDelegate;
            del.EndInvoke(result);
            //回调处理
            _ItemAsyncObject.IsCompleted = true;
        }

        /// <summary>
        /// 下载条码回调处理
        /// </summary>
        /// <param name="result"></param>
        private void BarcodeProxyCallback(IAsyncResult result) {
            AsyncResult async = (AsyncResult)result;
            DownloadDelegate del = (DownloadDelegate)async.AsyncDelegate;
            del.EndInvoke(result);
            //回调处理
            _BarcodeAsyncObject.IsCompleted = true;
        }

        /// <summary>
        /// 下载单据回调处理
        /// </summary>
        /// <param name="result"></param>
        private void SourceDocProxyCallback(IAsyncResult result) {
            AsyncResult async = (AsyncResult)result;
            DownloadDelegate del = (DownloadDelegate)async.AsyncDelegate;
            del.EndInvoke(result);
            //回调处理
            _SourceDocObject.IsCompleted = true;
        }
        #endregion

        #region 业务方法
        /// <summary>
        /// 1. CALL下载采购单服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetPurchaseOrderData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetPurchaseOrderService getPurchaseOrderSrv = this.GetService<IGetPurchaseOrderService>(this.TypeKey);
                if (conversionPara.ScanType == "1") {//扫描类型(scanType)=1.箱条码
                    QueryNode queryNode = OOQL.Select(
                                                        OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid", "ID"))
                                                    .From("BC_RECORD", "BC_RECORD")
                                                    .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                                            & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                    string id = this.GetService<IQueryService>().ExecuteScalar(queryNode).ToStringExtension();
                    dataTable = getPurchaseOrderSrv.GetPurchaseOrder(conversionPara.ProgramJobNo, "A", "1", new string[]{string.Empty}, id, conversionPara.SiteNo);
                } else if (conversionPara.ScanType == "2") {//扫描类型(scanType)=2 单据条码
                    dataTable = getPurchaseOrderSrv.GetPurchaseOrder(conversionPara.ProgramJobNo, "A", "2", conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                }
                _SourceDocObject.Result = dataTable;
            } catch (Exception  ex) {
                _SourceDocObject.IsHappenException = true;
                _SourceDocObject.Exception = ex;
            }
        }

        /// <summary>
        /// 2. CALL下载采购订单品号服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetPurchaseOrderItemData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetItemService getItemSrv = this.GetService<IGetItemService>(this.TypeKey);
                if (conversionPara.ScanType == "1") {//扫描类型(scanType)=1.箱条码
                    QueryNode queryNode = OOQL.Select(
                                                        OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid", "main_organization"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.RTK", "main_organization_type"))
                                                    .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                                                    .Where((OOQL.AuthFilter("PURCHASE_ORDER", "PURCHASE_ORDER")) &
                                                                (OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID").In(OOQL.Select(
                                                                            OOQL.CreateProperty("SOURCE_ID.ROid"))
                                                                        .From("BC_RECORD")
                                                                        .Where((OOQL.CreateProperty("BARCODE_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String))))));
                    DependencyObjectCollection ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                    queryNode = OOQL.Select(true,
                                                        OOQL.CreateProperty("BC_RECORD.ITEM_ID", "item_id"))
                                                    .From("BC_RECORD", "BC_RECORD")
                                                    .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                                        & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(conversionPara.DocNo)));
                    DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                    if (ownerOrg.Count > 0 && itemCollection.Count>0) {
                        dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, ownerOrg[0]["main_organization"]
                            , conversionPara.SiteNo, ownerOrg[0]["main_organization_type"].ToStringExtension());
                    }
                } else if (conversionPara.ScanType == "2") {//扫描类型(scanType)=2 单据条码
                    QueryNode queryNode = OOQL.Select(
                                                        OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid", "main_organization"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.RTK", "main_organization_type"))
                                                    .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                                                    .Where((OOQL.AuthFilter("PURCHASE_ORDER", "PURCHASE_ORDER")) &
                                                                (OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                    DependencyObjectCollection ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织

                    queryNode = OOQL.Select(true,
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID", "item_id"))
                                                    .From("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                                                    .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                                                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")))
                                                    .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                                                    .On((OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                                                    .LeftJoin("PLANT", "PLANT")
                                                    .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                                                    .Where((OOQL.AuthFilter("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D"))
                                                        & (OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String))
                                                        & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(conversionPara.SiteNo, GeneralDBType.String)));
                    DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                    if (ownerOrg.Count > 0 && itemCollection.Count>0) {
                        dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, ownerOrg[0]["main_organization"],
                            conversionPara.SiteNo, ownerOrg[0]["main_organization_type"].ToStringExtension());
                    }
                }
                _ItemAsyncObject.Result = dataTable;
            } catch (Exception  ex) {
                _ItemAsyncObject.IsHappenException = true;
                _ItemAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 3. CALL下载采购订单条码服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetPurchaseOrderBarcodeData(ConversionParameter conversionPara) {
            try {
                IGetBarcodeService getBarcodeSrv = this.GetService<IGetBarcodeService>(this.TypeKey);
                if (conversionPara.ScanType== "2") {
                    QueryNode queryNode = OOQL.Select(true,
                                                            OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID", "item_id"),
                                                            OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID", "item_feature_id"))
                                                        .From("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                                                        .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                                                        .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")))
                                                        .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                                                        .On((OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                                                        .LeftJoin("PLANT", "PLANT")
                                                        .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                                                        .Where((OOQL.AuthFilter("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D"))
                                                            & (OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String))
                                                            & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(conversionPara.SiteNo, GeneralDBType.String)));
                    DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID
                    if (itemCollection.Count > 0) {
                        List<object> itemIds = new List<object>();
                        List<object> itemFeatureIds = new List<object>();
                        foreach (var item in itemCollection) {
                            itemIds.Add(item["item_id"]);
                            itemFeatureIds.Add(item["item_feature_id"]);
                        }
                        _BarcodeAsyncObject.Result = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType,
                            itemIds, itemFeatureIds, conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
                    }
                }
                if (conversionPara.ScanType=="1") {
                    _BarcodeAsyncObject.Result = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType,
                    new List<object>(), new List<object>(), conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
                }
            } catch (Exception  ex) {
                _BarcodeAsyncObject.IsHappenException = true;
                _BarcodeAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 1. CALL下载到货单服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetPurchaseArrivalData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetPurchaseArrivalService getPurchaseArrivalSrv = this.GetService<IGetPurchaseArrivalService>(this.TypeKey);
                dataTable = getPurchaseArrivalSrv.GetPurchaseArrival(conversionPara.ProgramJobNo, "2", "A", conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                _SourceDocObject.Result = dataTable;
            } catch (Exception  ex) {
                _SourceDocObject.IsHappenException = true;
                _SourceDocObject.Exception = ex;
            }
        }

        /// <summary>
        /// 2. CALL下载到货单品号服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetPurchaseArrlivalItemData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetItemService getItemSrv = this.GetService<IGetItemService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(true,
                                                           OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_ID", "item_id"))
                                                       .From("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                                                       .InnerJoin("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                                                       .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID")))
                                                       .Where((OOQL.AuthFilter("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D"))
                                                           & (OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                queryNode = OOQL.Select(
                                                    OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.ROid", "main_organization"),
                                                            OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.RTK", "main_organization_type"))
                                                .From("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                                                .Where((OOQL.AuthFilter("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")) &
                                                            (OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织
                if (ownerOrg.Count > 0 && itemCollection.Count > 0) {
                    dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, ownerOrg[0]["main_organization"],
                        conversionPara.SiteNo, ownerOrg[0]["main_organization_type"].ToStringExtension());
                }
                _ItemAsyncObject.Result = dataTable;
            } catch (Exception  ex) {
                _ItemAsyncObject.IsHappenException = true;
                _ItemAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 3. CALL下载到货单条码服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetPurchaseArrlivalBarcodeData(ConversionParameter conversionPara) {
            try {
                IGetBarcodeService getBarcodeSrv = this.GetService<IGetBarcodeService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(true,
                                                        OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_ID", "item_id"),
                                                        OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_FEATURE_ID", "item_feature_id"))
                                                    .From("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                                                    .InnerJoin("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                                                    .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID")))
                                                    .Where((OOQL.AuthFilter("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D"))
                                                        & (OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID
                if (itemCollection.Count > 0) {
                    List<object> itemIds = new List<object>();
                    List<object> itemFeatureIds = new List<object>();
                    foreach (var item in itemCollection) {
                        itemIds.Add(item["item_id"]);
                        itemFeatureIds.Add(item["item_feature_id"]);
                    }
                    _BarcodeAsyncObject.Result = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType,
                        itemIds, itemFeatureIds, conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
                }
            } catch (Exception  ex) {
                _BarcodeAsyncObject.IsHappenException = true;
                _BarcodeAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 1. CALL下载销货单服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetSalesDeliveryData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetSalesDeliveryService getSalesDeliverySrv = this.GetService<IGetSalesDeliveryService>(this.TypeKey);
                dataTable = getSalesDeliverySrv.GetSalesDelivery(conversionPara.ProgramJobNo, "A", "2", conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                _SourceDocObject.Result = dataTable;
            } catch (Exception  ex) {
                _SourceDocObject.IsHappenException = true;
                _SourceDocObject.Exception = ex;
            }
        }

        /// <summary>
        /// 2. CALL下载销货单品号服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetSalesDeliveryItemData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetItemService getItemSrv = this.GetService<IGetItemService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(
                                                    OOQL.CreateProperty("SALES_DELIVERY.Owner_Org.ROid", "main_organization"),
                                                            OOQL.CreateProperty("SALES_DELIVERY.Owner_Org.RTK", "main_organization_type"))
                                                .From("SALES_DELIVERY", "SALES_DELIVERY")
                                                .Where((OOQL.AuthFilter("SALES_DELIVERY", "SALES_DELIVERY")) &
                                                            (OOQL.CreateProperty("SALES_DELIVERY.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织

                queryNode = OOQL.Select(true,
                                                            OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_ID", "item_id"))
                                                        .From("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D")
                                                        .InnerJoin("SALES_DELIVERY", "SALES_DELIVERY")
                                                        .On((OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_ID") == OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_ID")))
                                                        .Where((OOQL.AuthFilter("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D"))
                                                            & (OOQL.CreateProperty("SALES_DELIVERY.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID
                if (itemCollection.Count > 0 && ownerOrg.Count > 0) {
                    dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, ownerOrg[0]["main_organization"],
                        conversionPara.SiteNo, ownerOrg[0]["main_organization_type"].ToStringExtension());
                }
                _ItemAsyncObject.Result = dataTable;
            } catch (Exception  ex) {
                _ItemAsyncObject.IsHappenException = true;
                _ItemAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 3. CALL下载销货单条码服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetSalesDeliveryBarcodeData(ConversionParameter conversionPara) {
            try {
                IGetBarcodeService getBarcodeSrv = this.GetService<IGetBarcodeService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(true,
                                                        OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_ID", "item_id"),
                                                        OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_FEATURE_ID", "item_feature_id"))
                                                    .From("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D")
                                                    .InnerJoin("SALES_DELIVERY", "SALES_DELIVERY")
                                                    .On((OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_ID") == OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_ID")))
                                                    .Where((OOQL.AuthFilter("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D"))
                                                        & (OOQL.CreateProperty("SALES_DELIVERY.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection collection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID
                if (collection.Count > 0) {
                    List<object> itemIds = new List<object>();
                    List<object> itemFeatureIds = new List<object>();
                    foreach (var item in collection) {
                        itemIds.Add(item["item_id"]);
                        itemFeatureIds.Add(item["item_feature_id"]);
                    }
                    _BarcodeAsyncObject.Result = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType,
                        itemIds, itemFeatureIds, conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
                }
            } catch (Exception  ex) {
                _BarcodeAsyncObject.IsHappenException = true;
                _BarcodeAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 1. CALL下载领退料工单服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetIssueReceiptMoData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetIssueReceiptMoService getIssueReceiptMoSrv = this.GetService<IGetIssueReceiptMoService>(this.TypeKey);
                dataTable = getIssueReceiptMoSrv.GetIssueReceiptMo(conversionPara.ProgramJobNo, "2", conversionPara.Status, conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                _SourceDocObject.Result = dataTable;
            } catch (Exception  ex) {
                _SourceDocObject.IsHappenException = true;
                _SourceDocObject.Exception = ex;
            }
        }

        /// <summary>
        /// 2. CALL下载领退料工单品号服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetIssueReceiptMoItemData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetItemService getItemSrv = this.GetService<IGetItemService>(this.TypeKey);

                //品号、主营组织
                DependencyObjectCollection itemCollection = null, ownerOrg = null;

                if (conversionPara.Status == "A") {//IF 【入参status】=A.新增
                    QueryNode queryNode = OOQL.Select(true,
                                                        OOQL.CreateProperty("MO_D.ITEM_ID", "item_id"))
                                                    .From("MO.MO_D", "MO_D")
                                                    .InnerJoin("MO", "MO")
                                                    .On((OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_D.MO_ID")))
                                                    .Where((OOQL.AuthFilter("MO.MO_D", "MO_D"))
                                                        & (OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                    itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                    queryNode = OOQL.Select(
                                                    OOQL.CreateProperty("MO.Owner_Org.ROid", "main_organization"),
                                                            OOQL.CreateProperty("MO.Owner_Org.RTK", "main_organization_type"))
                                                .From("MO", "MO")
                                                .Where((OOQL.AuthFilter("MO", "MO")) &
                                                            (OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                    ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织

                } else if (conversionPara.Status == "S") {//【入参status】=S.过帐
                    QueryNode queryNode = OOQL.Select(true,
                                                                OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID", "item_id"))
                                                            .From("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                                                            .InnerJoin("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                                                            .On((OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID")))
                                                            .Where((OOQL.AuthFilter("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D"))
                                                                & (OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                    itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                    queryNode = OOQL.Select(
                                                    OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.ROid", "main_organization"),
                                                            OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.RTK", "main_organization_type"))
                                                .From("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                                                .Where((OOQL.AuthFilter("ISSUE_RECEIPT", "ISSUE_RECEIPT")) &
                                                            (OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                    ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织
                }

                if (itemCollection != null && ownerOrg != null && itemCollection.Count > 0 && ownerOrg.Count > 0) {
                    dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, ownerOrg[0]["main_organization"],
                        conversionPara.SiteNo, ownerOrg[0]["main_organization_type"].ToStringExtension());
                }
                _ItemAsyncObject.Result = dataTable;
            } catch (Exception ex) {
                _ItemAsyncObject.IsHappenException = true;
                _ItemAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 3. CALL下载领退料工单条码服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetIssueReceiptMoBarcodeData(ConversionParameter conversionPara) {
            try {
                IGetBarcodeService getBarcodeSrv = this.GetService<IGetBarcodeService>(this.TypeKey);
                QueryNode queryNode = null;
                if (conversionPara.Status == "A") {//IF 【入参status】=A.新增
                    queryNode = OOQL.Select(true,
                                                        OOQL.CreateProperty("MO_D.ITEM_ID", "item_id"),
                                                        OOQL.CreateProperty("MO_D.ITEM_FEATURE_ID", "item_feature_id"))
                                                    .From("MO.MO_D", "MO_D")
                                                    .InnerJoin("MO", "MO")
                                                    .On((OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_D.MO_ID")))
                                                    .Where((OOQL.AuthFilter("MO.MO_D", "MO_D"))
                                                        & (OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                } else if (conversionPara.Status == "S") {//【入参status】=S.过帐
                    queryNode = OOQL.Select(true,
                                                                OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID", "item_id"),
                                                        OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_FEATURE_ID", "item_feature_id"))
                                                            .From("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                                                            .InnerJoin("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                                                            .On((OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID")))
                                                            .Where((OOQL.AuthFilter("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D"))
                                                                & (OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                }
                DependencyObjectCollection collection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID
                if (collection.Count > 0) {
                    List<object> itemIds = new List<object>();
                    List<object> itemFeatureIds = new List<object>();
                    foreach (var item in collection) {
                        itemIds.Add(item["item_id"]);
                        itemFeatureIds.Add(item["item_feature_id"]);
                    }
                    _BarcodeAsyncObject.Result = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType,
                        itemIds, itemFeatureIds, conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
                }
            } catch (Exception ex) {
                _BarcodeAsyncObject.IsHappenException = true;
                _BarcodeAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 1. CALL获取入库工单服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetMoProductMoData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetMOProductMoService getMoProductSrv = this.GetService<IGetMOProductMoService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(
                                                    OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid", "ID"))
                                                .From("BC_RECORD", "BC_RECORD")
                                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD")) &
                                                            (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                string id = this.GetService<IQueryService>().ExecuteScalar(queryNode).ToStringExtension();

                dataTable = getMoProductSrv.GetMOProduct(conversionPara.ProgramJobNo, "1", "A", new string[] { string.Empty }, id, conversionPara.SiteNo); //20170724 modi by shenbao for P001-170717001 改为传入数组
                _SourceDocObject.Result = dataTable;
            } catch (Exception ex) {
                _SourceDocObject.IsHappenException = true;
                _SourceDocObject.Exception = ex;
            }
        }

        /// <summary>
        /// 2. CALL下载入库工单品号服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetMoProductMoItemData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetItemService getItemSrv = this.GetService<IGetItemService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(true,
                                                    OOQL.CreateProperty("BC_RECORD.ITEM_ID", "item_id"))
                                                .From("BC_RECORD", "BC_RECORD")
                                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                                    & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                queryNode = OOQL.Select(
                                                OOQL.CreateProperty("MO.Owner_Org.ROid", "main_organization"),
                                                        OOQL.CreateProperty("MO.Owner_Org.RTK", "main_organization_type"))
                                            .From("MO", "MO")
                                            .Where((OOQL.AuthFilter("MO", "MO")) &
                                                        (OOQL.CreateProperty("MO.MO_ID").In(OOQL.Select(
                                                                        OOQL.CreateProperty("SOURCE_ID.ROid"))
                                                                    .From("BC_RECORD")
                                                                    .Where((OOQL.CreateProperty("BARCODE_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String))))));
                DependencyObjectCollection ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织

                if (itemCollection.Count > 0 && ownerOrg.Count > 0) {
                    dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, ownerOrg[0]["main_organization"],
                        conversionPara.SiteNo, ownerOrg[0]["main_organization_type"].ToStringExtension());
                }
                _ItemAsyncObject.Result = dataTable;
            } catch (Exception ex) {
                _ItemAsyncObject.IsHappenException = true;
                _ItemAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 3. CALL下载入库工单条码服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetMoProductMoBarcodeData(ConversionParameter conversionPara) {
            try {
                IGetBarcodeService getBarcodeSrv = this.GetService<IGetBarcodeService>(this.TypeKey);
                _BarcodeAsyncObject.Result = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType,
                    new List<object>(), new List<object>(), conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
            } catch (Exception ex) {
                _BarcodeAsyncObject.IsHappenException = true;
                _BarcodeAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 1. CALL获取杂发、杂收服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetTransactionDocData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetTransactionDocService getTransactionDocSrv = this.GetService<IGetTransactionDocService>(this.TypeKey);

                dataTable = getTransactionDocSrv.GetTransactionDoc(conversionPara.ProgramJobNo, "2", "S", conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
                _SourceDocObject.Result = dataTable;
            } catch (Exception  ex) {
                _SourceDocObject.IsHappenException = true;
                _SourceDocObject.Exception = ex;
            }
        }

        /// <summary>
        /// 2. CALL下载杂发、杂收品号服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetTransactionDocItemData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetItemService getItemSrv = this.GetService<IGetItemService>(this.TypeKey);

                QueryNode queryNode = OOQL.Select(true,
                                                    OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID", "item_id"))
                                                .From("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                                                .InnerJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
                                                .On((OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID")))
                                                .Where((OOQL.AuthFilter("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D"))
                                                    & (OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                queryNode = OOQL.Select(
                                                   OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.ROid", "main_organization"),
                                                            OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.RTK", "main_organization_type"))
                                               .From("TRANSACTION_DOC", "TRANSACTION_DOC")
                                               .Where((OOQL.AuthFilter("TRANSACTION_DOC", "TRANSACTION_DOC")) &
                                                           (OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织


                if (itemCollection.Count > 0 && ownerOrg.Count > 0) {
                    dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, ownerOrg[0]["main_organization"],
                        conversionPara.SiteNo, ownerOrg[0]["main_organization_type"].ToStringExtension());
                }
                _ItemAsyncObject.Result = dataTable;
            } catch (Exception ex) {
                _ItemAsyncObject.IsHappenException = true;
                _ItemAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 3. CALL下载杂发、杂收条码服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetTransactionDocBarcodeData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetBarcodeService getBarcodeSrv = this.GetService<IGetBarcodeService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(true,
                                                    OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID", "item_id"),
                                                    OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_FEATURE_ID", "item_feature_id"))
                                                .From("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                                                .InnerJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
                                                .On((OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID")))
                                                .Where((OOQL.AuthFilter("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D"))
                                                    & (OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection collection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);
                if (collection.Count > 0) {
                    List<object> itemIds = new List<object>();
                    List<object> itemFeatureIds = new List<object>();
                    foreach (var item in collection) {
                        itemIds.Add(item["item_id"]);
                        itemFeatureIds.Add(item["item_feature_id"]);
                    }
                    dataTable = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType, itemIds, itemFeatureIds,
                        conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
                }
                _BarcodeAsyncObject.Result = dataTable;
            } catch (Exception ex) {
                _BarcodeAsyncObject.IsHappenException = true;
                _BarcodeAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 2. CALL下载库存调拨单品号服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetTransferDocItemData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetItemService getItemSrv = this.GetService<IGetItemService>(this.TypeKey);

                QueryNode queryNode = OOQL.Select(true,
                                                    OOQL.CreateProperty("BC_RECORD.ITEM_ID", "item_id"))
                                                .From("BC_RECORD", "BC_RECORD")
                                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                                    & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                //queryNode = OOQL.Select(
                //                                   OOQL.CreateProperty("TRANSFER_DOC.Owner_Org.ROid", "main_organization"),
                //                                            OOQL.CreateProperty("TRANSFER_DOC.Owner_Org.RTK", "main_organization_type"))
                //                               .From("TRANSFER_DOC", "TRANSFER_DOC")
                //                               .Where((OOQL.AuthFilter("TRANSFER_DOC", "TRANSFER_DOC")) &
                //                                           (OOQL.CreateProperty("TRANSFER_DOC.TRANSFER_DOC_ID").In(OOQL.Select(
                //                                                            OOQL.CreateProperty("SOURCE_ID.ROid"))
                //                                                        .From("BC_RECORD")
                //                                                        .Where((OOQL.CreateProperty("BARCODE_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String))))));
                //DependencyObjectCollection ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织

                queryNode = OOQL.Select(
                                                   OOQL.CreateProperty("PLANT.PLANT_ID", "main_organization"))
                                               .From("PLANT", "PLANT")
                                               .Where((OOQL.AuthFilter("PLANT", "PLANT")) &
                                                           (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(conversionPara.SiteNo, GeneralDBType.String)));
                object plantId = this.GetService<IQueryService>().ExecuteScalar(queryNode);//工厂
                if (itemCollection.Count > 0
                    //&& ownerOrg.Count > 0
                    ) {
                    dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, plantId,
                    conversionPara.SiteNo, "PLANT");
                }
                _ItemAsyncObject.Result = dataTable;
            } catch (Exception ex) {
                _ItemAsyncObject.IsHappenException = true;
                _ItemAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 3. CALL下载库存调拨单条码服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetTransferDocBarcodeData(ConversionParameter conversionPara) {
            try {
                IGetBarcodeService getBarcodeSrv = this.GetService<IGetBarcodeService>(this.TypeKey);
                _BarcodeAsyncObject.Result = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType,
                    new List<object>(), new List<object>(), conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
            } catch (Exception ex) {
                _BarcodeAsyncObject.IsHappenException = true;
                _BarcodeAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 1. CALL获取库存拨入单服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetTransferInDocData(ConversionParameter conversionPara) {
            try {
                IGetTransferDocService getTransferDocSrv = this.GetService<IGetTransferDocService>(this.TypeKey);
                _SourceDocObject.Result = getTransferDocSrv.GetTransferDoc(conversionPara.ProgramJobNo, "2", conversionPara.Status,
                    conversionPara.DocNo, Maths.GuidDefaultValue().ToString(), conversionPara.SiteNo);
            } catch (Exception ex) {
                _SourceDocObject.IsHappenException = true;
                _SourceDocObject.Exception = ex;
            }
        }

        /// <summary>
        /// 2. CALL下载库存拨入单品号服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetTransferInDocItemData(ConversionParameter conversionPara) {
            DependencyObjectCollection dataTable = null;
            try {
                IGetItemService getItemSrv = this.GetService<IGetItemService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(true,
                                                    OOQL.CreateProperty("TRANSFER_DOC_D.ITEM_ID", "item_id"))
                                                .From("TRANSFER_DOC.TRANSFER_DOC_D", "TRANSFER_DOC_D")
                                                .InnerJoin("TRANSFER_DOC", "TRANSFER_DOC")
                                                .On((OOQL.CreateProperty("TRANSFER_DOC.TRANSFER_DOC_ID") == OOQL.CreateProperty("TRANSFER_DOC_D.TRANSFER_DOC_ID")))
                                                .Where((OOQL.AuthFilter("TRANSFER_DOC.TRANSFER_DOC_D", "TRANSFER_DOC_D"))
                                                    & (OOQL.CreateProperty("TRANSFER_DOC.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection itemCollection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID

                queryNode = OOQL.Select(
                                                   OOQL.CreateProperty("TRANSFER_DOC.Owner_Org.ROid", "main_organization"),
                                                            OOQL.CreateProperty("TRANSFER_DOC.Owner_Org.RTK", "main_organization_type"))
                                               .From("TRANSFER_DOC", "TRANSFER_DOC")
                                               .Where((OOQL.AuthFilter("TRANSFER_DOC", "TRANSFER_DOC")) &
                                                           (OOQL.CreateProperty("TRANSFER_DOC.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection ownerOrg = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//主营组织

                if (itemCollection.Count > 0 && ownerOrg.Count > 0) {
                    dataTable = getItemSrv.GetItem(conversionPara.ProgramJobNo, itemCollection, ownerOrg[0]["main_organization"],
                        conversionPara.SiteNo, ownerOrg[0]["main_organization_type"].ToStringExtension());
                }
                _ItemAsyncObject.Result = dataTable;
            } catch (Exception  ex) {
                _ItemAsyncObject.IsHappenException = true;
                _ItemAsyncObject.Exception = ex;
            }
        }

        /// <summary>
        /// 3. CALL下载库存调拨单条码服务
        /// </summary>
        /// <param name="conversionPara">接口转换服务参数</param>
        private void GetTransferInDocBarcodeData(ConversionParameter conversionPara) {
            try {
                IGetBarcodeService getBarcodeSrv = this.GetService<IGetBarcodeService>(this.TypeKey);
                QueryNode queryNode = OOQL.Select(true,
                                                    OOQL.CreateProperty("TRANSFER_DOC_D.ITEM_ID", "item_id"),
                                                    OOQL.CreateProperty("TRANSFER_DOC_D.ITEM_FEATURE_ID", "item_feature_id"))
                                                .From("TRANSFER_DOC.TRANSFER_DOC_D", "TRANSFER_DOC_D")
                                                .InnerJoin("TRANSFER_DOC", "TRANSFER_DOC")
                                                .On((OOQL.CreateProperty("TRANSFER_DOC.TRANSFER_DOC_ID") == OOQL.CreateProperty("TRANSFER_DOC_D.TRANSFER_DOC_ID")))
                                                .Where((OOQL.AuthFilter("TRANSFER_DOC.TRANSFER_DOC_D", "TRANSFER_DOC_D"))
                                                    & (OOQL.CreateProperty("TRANSFER_DOC.DOC_NO") == OOQL.CreateConstants(conversionPara.DocNo, GeneralDBType.String)));
                DependencyObjectCollection collection = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);//品号ID
                if (collection.Count > 0) {
                    List<object> itemIds = new List<object>();
                    List<object> itemFeatureIds = new List<object>();
                    foreach (var item in collection) {
                        itemIds.Add(item["item_id"]);
                        itemFeatureIds.Add(item["item_feature_id"]);
                    }
                    _BarcodeAsyncObject.Result = getBarcodeSrv.GetBarcode(conversionPara.DocNo, conversionPara.ScanType,
                        itemIds, itemFeatureIds, conversionPara.SiteNo, conversionPara.ProgramJobNo, conversionPara.Status);
                }
            } catch (Exception  ex) {
                _BarcodeAsyncObject.IsHappenException = true;
                _BarcodeAsyncObject.Exception = ex;
            }
        }
        #endregion
    }
}

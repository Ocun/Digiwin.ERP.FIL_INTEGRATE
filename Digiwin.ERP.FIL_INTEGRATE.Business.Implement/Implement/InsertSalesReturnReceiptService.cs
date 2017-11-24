//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2017-02-13</createDate>
//<description>生成销退入库单服务</description>
//---------------------------------------------------------------- 
//20170330 modi by wangrm for P001-170328001
//20170619 modi by zhangcn for P001-170606002

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Data;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using Digiwin.ERP.EFNET.Business;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 生成销退入库单
    /// </summary>
    [ServiceClass(typeof(IInsertSalesReturnReceiptService))]
    [Description("生成销退入库单")]
    public class InsertSalesReturnReceiptService : ServiceComponent, IInsertSalesReturnReceiptService {
        #region 属性
        private IQueryService _querySrv;
        private IBusinessTypeService _businessTypeSrv;
        private QueryNode _queryNode;
        private IDataEntityType _TEMP_SCAN;
        private IDataEntityType _TEMP_SCAN_DETAIL;
        private DependencyObjectCollection _collDocNos;
        private Dictionary<object, decimal> _dicSumBusinessQty;

        private IInfoEncodeContainer _encodeSrv;
        /// <summary>
        /// 信息编码服务
        /// </summary>
        public IInfoEncodeContainer InfoEncodeSrv {
            get { return _encodeSrv ?? (_encodeSrv = GetService<IInfoEncodeContainer>()); }
        }

        private IPrimaryKeyService _primaryKeySrv;
        /// <summary>
        /// 主键生成服务
        /// </summary>
        public IPrimaryKeyService PrimaryKeySrv {
            get { return _primaryKeySrv ?? (_primaryKeySrv = GetService<IPrimaryKeyService>("SALES_RETURN_RECEIPT")); }
        }

        private IDocumentNumberGenerateService _documentNumberGenSrv;
        /// <summary>
        /// 生成单号服务
        /// </summary>
        public IDocumentNumberGenerateService DocumentNumberGenSrv {
            get {
                return _documentNumberGenSrv ??
                       (_documentNumberGenSrv = GetService<IDocumentNumberGenerateService>("SALES_RETURN_RECEIPT"));
            }
        }

        #endregion

        #region IInsertSalesReturnReceiptService接口成员

        /// <summary>
        /// 生成销退入库单服务
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collScan">参数集合</param>
        /// <returns>新生成的单号DocNo</returns>
        public DependencyObjectCollection DoInsertSalesReturnReceipt(string employeeNo, string scanType,
            DateTime reportDatetime, string pickingDepartmentNo, string recommendedOperations, string recommendedFunction,
            string scanDocNo, DependencyObjectCollection collScan) {
            #region 参数检查
            if (Maths.IsEmpty(recommendedOperations)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "recommended_operations"));//‘入参【recommended_operations】未传值’
            }
            if (Maths.IsEmpty(recommendedFunction)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "recommended_function"));//‘入参【recommended_function】未传值’
            }

            #endregion

            #region 准备临时表的列映射与数据
            _dicSumBusinessQty = new Dictionary<object, decimal>();
            _collDocNos = CreateReturnCollection();//创建返回值的集合
            List<object> lstSalesReturnReceiptIds = new List<object>();//记录 新生的【销退入库单】的主键

            string category = "15"; //销退入库单
            DateTime docDate = reportDatetime;

            DataTable dtTempScan = null;//临时表Scan的DataTable结构
            DataTable dtTempScanDetail = null;//临时表ScanDetail的DataTable结构
            List<BulkCopyColumnMapping> lstColumnMappingsScan = null;//临时表Scan的映射列
            List<BulkCopyColumnMapping> lstColumnMappingsScanDetail = null;//临时表ScanDetail的映射列
            CreateTempTableMapping(ref dtTempScan, ref lstColumnMappingsScan, ref dtTempScanDetail, ref lstColumnMappingsScanDetail);//创建临时表#Table_scan的列映射
            InsertTempTableData(dtTempScan, dtTempScanDetail, collScan, employeeNo, pickingDepartmentNo);//插入DataTable数据

            #endregion

            #region 生成销退入库单

            using (var transService = GetService<ITransactionService>()) {
                using (var myConnectionSr = GetService<IConnectionService>()) {
                    InitParameters();//初始化全局变量

                    #region 7.1查询基础资料，供生单使用

                    //创建临时表Scan与ScanDetail
                    CreateTempTableScan();
                    CreateTempTableScanDetail();
                    _querySrv.BulkCopy(dtTempScan, _TEMP_SCAN.Name, lstColumnMappingsScan.ToArray());//批量插入数据到临时表Scan
                    _querySrv.BulkCopy(dtTempScanDetail, _TEMP_SCAN_DETAIL.Name, lstColumnMappingsScanDetail.ToArray());//批量插入数据到临时表ScanDetail

                    #endregion

                    #region 7.2生成销退入库单实体，调用平台的保存服务生成单据(自动审核的单据平台会自动审核)
                    object docId =  ValidateParaFilDoc(collScan, category);//校验单据类型
                    InsertSalesReturnReceipt(category, reportDatetime, lstSalesReturnReceiptIds, docId);//7.2.1单头 销退入库单(SALES_RETURN_RECEIPT)
                    InsertSalesReturnReceiptD(); //7.2.2单身 (SALES_RETURN_RECEIPT_D)

                    DependencyObjectCollection collParaFil = QueryParaFil();
                    if (collParaFil.Count > 0 && collParaFil[0]["BC_LINE_MANAGEMENT"].ToBoolean()) {
                        InsertBcLine();//插入条码交易明细 BC_LINE
                    }
                    #endregion

                    #region 7.3自动签核
                    DependencyObjectCollection collSalesReturnReceipt = QuerySalesReturnReceipt();
                    if (collSalesReturnReceipt.Count > 0) {
                        string pProgramInfo = "SALES_RETURN_RECEIPT.I01";
                        IEFNETStatusStatusService efnetSrv = GetService<IEFNETStatusStatusService>();
                        foreach (DependencyObject objSalesReturnReceipt in collSalesReturnReceipt) {
                            efnetSrv.GetFormFlow(pProgramInfo, objSalesReturnReceipt["DOC_ID"], objSalesReturnReceipt["Owner_Org_ROid"],
                                 new List<object>() { objSalesReturnReceipt["SALES_RETURN_RECEIPT_ID"] });
                        }
                    }
                    #endregion
                }

                //保存单据
                IReadService readSrv = GetService<IReadService>("SALES_RETURN_RECEIPT");
                object[] entities = readSrv.Read(lstSalesReturnReceiptIds.ToArray());
                if (entities != null && entities.Length > 0) {
                    ISaveService saveSrv = GetService<ISaveService>("SALES_RETURN_RECEIPT");
                    saveSrv.Save(entities);
                }

                transService.Complete(); //事务提交
            }

            #endregion

            return _collDocNos;
        }
        #endregion

        #region 业务方法

        /// <summary>
        /// 初始化全局变量
        /// </summary>
        private void InitParameters() {
            _querySrv = GetService<IQueryService>();
            _businessTypeSrv = GetServiceForThisTypeKey<IBusinessTypeService>();
        }

        /// <summary>
        /// 创建临时表#Table_scan的列映射
        /// </summary>
        /// <param name="dtTempScan"></param>
        /// <param name="lstColumnMappingsScan"></param>
        /// <param name="dtTempScanDetail"></param>
        /// <param name="lstColumnMappingsScanDetail"></param>
        private void CreateTempTableMapping(ref DataTable dtTempScan, ref List<BulkCopyColumnMapping> lstColumnMappingsScan, ref DataTable dtTempScanDetail, ref List<BulkCopyColumnMapping> lstColumnMappingsScanDetail) {
            #region Scan表
            string[] scanColumns = new string[]{
                    "Table_scan_ID",  //主键
                    "site_no",  //工厂 
                    "info_lot_no",  //信息批号
                    "employee_no",  //扫描人员
                    "picking_department_no" //领料部门 
            };
            dtTempScan = UtilsClass.CreateDataTable("", scanColumns,
                    new Type[]{
                        typeof(object),    //主键
                        typeof(string),  //工厂
                        typeof(string),  //信息批号
                        typeof(string),  //扫描人员
                        typeof(string)   //领料部门
                       
                    });

            //创建map对照表
            Dictionary<string, string> dicScan = new Dictionary<string, string>();
            foreach (string key in scanColumns) {
                dicScan.Add(key, key);
            }
            lstColumnMappingsScan = UtilsClass.CreateBulkMapping(dicScan);
            #endregion

            #region ScanDetail表
            string[] scanColumnsDetail = new string[]{
                    "Table_scan_detail_ID",  //主键
                    "site_no",  
                    "info_lot_no",  //信息批号
                    "barcode_no",  
                    "item_no",
                    "item_feature_no",                   
                    "warehouse_no",                   
                    "storage_spaces_no",                   
                    "lot_no",                    
                    "picking_qty",                    
                    "picking_unit_no",                   
                    "doc_no",                    
                    "seq" ,
                    "SequenceNumber"  
            };
            dtTempScanDetail = UtilsClass.CreateDataTable("", scanColumnsDetail,
                    new Type[]{
                        typeof(object),    //主键
                        typeof(string),  
                        typeof(string),  
                        typeof(string), 
                        typeof(string), 
                        typeof(string),  
                        typeof(string), 
                        typeof(string), 
                        typeof(string),  
                        typeof(decimal), 
                        typeof(string), 
                        typeof(string), 
                        typeof(int),
                        typeof(int)
                    });

            //创建map对照表
            Dictionary<string, string> dicScanDetail = new Dictionary<string, string>();
            foreach (string key in scanColumnsDetail) {
                dicScanDetail.Add(key, key);
            }
            lstColumnMappingsScanDetail = UtilsClass.CreateBulkMapping(dicScanDetail);
            #endregion
        }

        /// <summary>
        /// 插入临时表#Table_scan
        /// </summary>
        /// <param name="dtTempScan"></param>
        /// <param name="dtTempScanDetail"></param>
        /// <param name="collScan"></param>
        /// <param name="employeeNo"></param>
        /// <param name="pickingDepartmentNo"></param>
        private void InsertTempTableData(DataTable dtTempScan, DataTable dtTempScanDetail, DependencyObjectCollection collScan, string employeeNo, string pickingDepartmentNo) {
            foreach (DependencyObject objScan in collScan) {
                #region Scan
                DataRow drScan = dtTempScan.NewRow();
                drScan["Table_scan_ID"] = PrimaryKeySrv.CreateId();
                drScan["site_no"] = objScan["site_no"];
                drScan["info_lot_no"] = objScan["info_lot_no"];
                drScan["employee_no"] = employeeNo;
                drScan["picking_department_no"] = pickingDepartmentNo;

                dtTempScan.Rows.Add(drScan);//添加行

                #endregion

                #region ScanDetail 单头与单身中的info_lot_no有关联

                DependencyObjectCollection collScanDetail = objScan["scan_detail"] as DependencyObjectCollection;
                if (collScanDetail != null && collScanDetail.Count > 0) {
                    int sequenceNo = 1;
                    Dictionary<string, EntityLine> dicLineKey = new Dictionary<string, EntityLine>(); //根据唯一性字段来记录对应的行信息

                    foreach (DependencyObject objScanDetail in collScanDetail) {
                        //唯一性字段组合：信息批号+源单单号+序号+品号+特征码+仓库+库位+批号+单位+工厂
                        string uniqueKey = string.Concat(
                            objScanDetail["doc_no"].ToStringExtension(), objScanDetail["seq"].ToStringExtension(),
                            objScanDetail["item_no"].ToStringExtension(), objScanDetail["item_feature_no"].ToStringExtension(),
                            objScanDetail["warehouse_no"].ToStringExtension(), objScanDetail["storage_spaces_no"].ToStringExtension(),
                            objScanDetail["lot_no"].ToStringExtension(), objScanDetail["picking_unit_no"].ToStringExtension(),
                            objScanDetail["info_lot_no"].ToStringExtension(), objScanDetail["site_no"].ToStringExtension());

                        DataRow drScanDetail = dtTempScanDetail.NewRow();
                        if (!dicLineKey.ContainsKey(uniqueKey)) {  //新的一组，重新生成行主键和行号
                            EntityLine line = new EntityLine();
                            line.UniqueKey = uniqueKey;
                            line.Key = PrimaryKeySrv.CreateId();
                            line.SequenceNumber = sequenceNo++;

                            drScanDetail["Table_scan_detail_ID"] = line.Key;
                            drScanDetail["SequenceNumber"] = line.SequenceNumber;

                            dicLineKey.Add(uniqueKey, line);
                        }
                        else {  //已经存在的
                            drScanDetail["Table_scan_detail_ID"] = dicLineKey[uniqueKey].Key;
                            drScanDetail["SequenceNumber"] = dicLineKey[uniqueKey].SequenceNumber;
                        }

                        drScanDetail["site_no"] = objScanDetail["site_no"];
                        drScanDetail["info_lot_no"] = objScanDetail["info_lot_no"];
                        drScanDetail["barcode_no"] = objScanDetail["barcode_no"];
                        drScanDetail["item_no"] = objScanDetail["item_no"];
                        drScanDetail["item_feature_no"] = objScanDetail["item_feature_no"];
                        drScanDetail["warehouse_no"] = objScanDetail["warehouse_no"];
                        drScanDetail["storage_spaces_no"] = objScanDetail["storage_spaces_no"];
                        drScanDetail["lot_no"] = objScanDetail["lot_no"];
                        drScanDetail["picking_qty"] = objScanDetail["picking_qty"].ToDecimal();
                        drScanDetail["picking_unit_no"] = objScanDetail["picking_unit_no"];
                        drScanDetail["doc_no"] = objScanDetail["doc_no"];
                        drScanDetail["seq"] = objScanDetail["seq"].ToInt32();

                        dtTempScanDetail.Rows.Add(drScanDetail);//添加行

                        if (!_dicSumBusinessQty.ContainsKey(drScanDetail["info_lot_no"])) {
                            _dicSumBusinessQty.Add(drScanDetail["info_lot_no"], drScanDetail["picking_qty"].ToDecimal());
                        }
                        else {
                            _dicSumBusinessQty[drScanDetail["info_lot_no"]] = drScanDetail["picking_qty"].ToDecimal() +
                                                                              _dicSumBusinessQty[drScanDetail["info_lot_no"]];
                        }
                    }
                }
                #endregion
            }
        }

        #region 临时表
        /// <summary>
        /// 创建临时表#Table_scan
        /// </summary>
        /// <returns></returns>
        private void CreateTempTableScan() {
            string tempName = "TEMP_SCAN" + "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { });
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);

            #region 字段
            //主键
            defaultType.RegisterSimpleProperty("Table_scan_ID", _businessTypeSrv.SimplePrimaryKeyType,
                                               null, false, new Attribute[] { _businessTypeSrv.SimplePrimaryKey });
            //工厂 
            defaultType.RegisterSimpleProperty("site_no", _businessTypeSrv.SimpleFactoryType,
                                               string.Empty, false, new Attribute[] { _businessTypeSrv.SimpleFactory });
            //信息批号
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            //扫描人员 
            defaultType.RegisterSimpleProperty("employee_no", _businessTypeSrv.SimpleBusinessCodeType,
                                               string.Empty, false, new Attribute[] { _businessTypeSrv.SimpleBusinessCode });
            //领料部门
            defaultType.RegisterSimpleProperty("picking_department_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });

            #endregion

            _TEMP_SCAN = defaultType;

            _querySrv.CreateTempTable(defaultType);
        }

        /// <summary>
        /// 创建临时表#Table_scan_detail
        /// </summary>
        /// <returns></returns>
        private void CreateTempTableScanDetail() {
            string tempName = "TEMP_SCAN_DETAIL" + "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { });
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);

            #region 字段
            //主键
            defaultType.RegisterSimpleProperty("Table_scan_detail_ID", _businessTypeSrv.SimplePrimaryKeyType,
                                               null, false, new Attribute[] { _businessTypeSrv.SimplePrimaryKey });
            //信息批号
            defaultType.RegisterSimpleProperty("site_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            //信息批号
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });

            //条码编号
            defaultType.RegisterSimpleProperty("barcode_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });

            //料件编号
            defaultType.RegisterSimpleProperty("item_no", _businessTypeSrv.SimpleItemCodeType,
                                               string.Empty, false, new Attribute[] { _businessTypeSrv.SimpleItemCode });

            // 产品特征
            defaultType.RegisterSimpleProperty("item_feature_no", _businessTypeSrv.SimpleItemFeatureType,
                                               string.Empty, false, new Attribute[] { _businessTypeSrv.SimpleItemFeature });

            // 仓库
            defaultType.RegisterSimpleProperty("warehouse_no", typeof(string),
                                                string.Empty, false, new Attribute[] { tempAttr });
            //库位
            defaultType.RegisterSimpleProperty("storage_spaces_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            //批号
            defaultType.RegisterSimpleProperty("lot_no", _businessTypeSrv.SimpleLotCodeType,
                                               string.Empty, false, new Attribute[] { _businessTypeSrv.SimpleLotCode });
            //拣货数量
            defaultType.RegisterSimpleProperty("picking_qty", _businessTypeSrv.SimpleQuantityType,
                                               0m, false, new Attribute[] { _businessTypeSrv.SimpleQuantity });
            //拣货单位
            defaultType.RegisterSimpleProperty("picking_unit_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });

            //单号
            defaultType.RegisterSimpleProperty("doc_no", _businessTypeSrv.SimpleDocNoType, string.Empty, false, new Attribute[] { _businessTypeSrv.SimpleDocNo });

            //来源序号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("seq", typeof(int), 0, false, new Attribute[] { tempAttr });

            //序号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("SequenceNumber", typeof(int), 0, false, new Attribute[] { tempAttr });
            #endregion

            _TEMP_SCAN_DETAIL = defaultType;

            _querySrv.CreateTempTable(defaultType);
        }

        #endregion

        #region 插入操作

        /// <summary>
        /// 插入调拨单
        /// </summary>
        /// <param name="category">种类</param>
        /// <param name="reportDatetime">单据日期</param>
        /// <param name="lstSalesReturnReceiptIds"></param>
        private void InsertSalesReturnReceipt(string category, DateTime reportDatetime, List<object> lstSalesReturnReceiptIds, object docId) {
            DataTable dataTable = QueryTempScan(category, reportDatetime, docId);
           
            dataTable.TableName = "SALES_RETURN_RECEIPT";
            if (dataTable.Rows.Count > 0) {
                string tempDocNo = string.Empty; //临时变量，用于产生新的采购退货单单号
                Dictionary<string, string> dicDocNo = new Dictionary<string, string>(); //生成采购入库单单号字典

                foreach (DataRow dr in dataTable.Rows) {
                    lstSalesReturnReceiptIds.Add(dr["SALES_RETURN_RECEIPT_ID"]);
                    #region 处理计算列
                    string key_DocNo = dr["DOC_ID"].ToStringExtension() + dr["DOC_DATE"].ToDate();

                    if (!dicDocNo.ContainsKey(key_DocNo)) {
                        tempDocNo = UtilsClass.NextNumber(DocumentNumberGenSrv, "", dr["DOC_ID"], dr["SEQUENCE_DIGIT"].ToInt32(), DateTime.Now);

                        dicDocNo.Add(key_DocNo, tempDocNo);//加入字典
                    }
                    else {
                        tempDocNo = UtilsClass.NextNumber(DocumentNumberGenSrv, tempDocNo, dr["DOC_ID"], dr["SEQUENCE_DIGIT"].ToInt32(), DateTime.Now);

                        dicDocNo[key_DocNo] = tempDocNo;//更新字典
                    }
                    dr["DOC_NO"] = tempDocNo;
                    dr["VIEW_DOC_NO"] = tempDocNo;

                    decimal sumBusinessQty = 0m;//数量合计
                    if (_dicSumBusinessQty.ContainsKey(dr["info_lot_no"])) {
                        sumBusinessQty = _dicSumBusinessQty[dr["info_lot_no"]];
                    }
                    dr["SUM_BUSINESS_QTY"] = sumBusinessQty;

                    //20170619 add by zhangcn for P001-170606002 ===beigin===
                    if (Maths.IsNotEmpty(dr["GROUP_SYNERGY_ID_ROid"])){
                        DependencyObjectCollection collSalesSyneryFiD = QuerySalesSyneryFiD(dr["GROUP_SYNERGY_ID_ROid"]);
                        if (collSalesSyneryFiD.Count > 0){
                            dr["DOC_Sequence"] = collSalesSyneryFiD[0]["SequenceNumber"].ToInt32();
                            dr["GROUP_SYNERGY_D_ID"] = collSalesSyneryFiD[0]["SALES_SYNERGY_FI_D_ID"];
                        }
                    }
                    //20170619 add by zhangcn for P001-170606002 ===end===

                    #endregion

                    DependencyObject objDocNo = _collDocNos.AddNew();
                    objDocNo["doc_no"] = tempDocNo;  //用于主方法返回值
                }

                #region 删除执行批量插入时多余列（计算列做计算时需要用的的列）

                var calcColumn = new Collection<string>();
                calcColumn.Add("site_no");
                calcColumn.Add("info_lot_no");
                calcColumn.Add("SEQUENCE_DIGIT");

                foreach (var column in calcColumn) {
                    if (dataTable.Columns.Contains(column)) {
                        dataTable.Columns.Remove(column); //删除执行批量插入时多余列
                    }
                }
                #endregion

                //获取需要拆入数据表的ColumnMappings
                var mapping = GetBulkCopyColumnMapping(dataTable.Columns);
                _querySrv.BulkCopy(dataTable, dataTable.TableName, mapping.ToArray()); //批量插入数据库表中
            }
        }

        /// <summary>
        /// 插入子表
        /// </summary>
        private void InsertSalesReturnReceiptD() {
            DataTable dataTable = QueryTempScanDetail();
            if (dataTable.Rows.Count > 0) {
                foreach (DataRow dr in dataTable.Rows){
                    object salesReturnReceiptDId = dr["SALES_RETURN_RECEIPT_D_ID"];
                    object syneryId = dr["SYNERGY_ID"];
                    string innerSettlementClose = "0";
                    int syneryType = 0;
                    object syneryDId = Maths.GuidDefaultValue();

                    if (Maths.IsEmpty(syneryId)) {
                        innerSettlementClose = "0";// '0.不需要结算'
                        syneryType = 0;//协同关系类型
                        syneryDId = Maths.GuidDefaultValue();//协同序号ID
                    }
                    else {
                        innerSettlementClose = "1";// '1.需要结算'

                        #region 计算 协同关系类型
                        DependencyObjectCollection collSalesSynery = QuerySalesSynery(syneryId);
                        DependencyObjectCollection collSupplySynery = QuerySupplySynery(syneryId);
                        if (collSalesSynery.Count > 0) {
                            syneryType = 1;//1.集团销售
                        }
                        else if (collSupplySynery.Count > 0) {
                            string supplyType = collSupplySynery[0]["SUPPLY_TYPE"].ToStringExtension();
                            if (supplyType == "1") {
                                syneryType = 2;//2.集团采购
                            }
                            else if (supplyType == "2") {
                                syneryType = 3;//3.内部采购
                            }
                            else if (supplyType == "3") {
                                syneryType = 4;//4.内部配送
                            }
                        }
                        #endregion

                        #region 计算 协同序号ID
                        DependencyObjectCollection collSalesSyneryFiD = QuerySalesSyneryFiD(syneryId);
                        if (collSalesSyneryFiD.Count > 0) {
                            syneryDId = collSalesSyneryFiD[0]["SALES_SYNERGY_FI_D_ID"];//协同序号ID
                        }
                        else {
                            syneryDId = Maths.GuidDefaultValue();//协同序号ID
                        }
                        #endregion
                    }

                    #region 更新部分字段
                    List<SetItem> updateList = new List<SetItem>();
                    updateList.Add(new SetItem(OOQL.CreateProperty("INNER_SETTLEMENT_CLOSE"),OOQL.CreateConstants(innerSettlementClose)));
                    updateList.Add(new SetItem(OOQL.CreateProperty("SYNERGY_TYPE"), OOQL.CreateConstants(syneryType)));
                    updateList.Add(new SetItem(OOQL.CreateProperty("SYNERGY_D_ID"), OOQL.CreateConstants(syneryDId)));

                    QueryNode updateNode = 
                        OOQL.Update("SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_D",updateList.ToArray())
                            .Where(OOQL.CreateProperty("SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_D.SALES_RETURN_RECEIPT_D_ID") == 
                                   OOQL.CreateConstants(salesReturnReceiptDId));
                    #endregion

                    _querySrv.ExecuteNoQueryWithManageProperties(updateNode);
                }
            }
        }

        /// <summary>
        /// 插入子表
        /// </summary>
        private void InsertSalesReturnReceiptD_OLD() {
            DataTable dataTable = QueryTempScanDetail();
            dataTable.TableName = "SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_D";
            if (dataTable.Rows.Count > 0) {
                foreach (DataRow dr in dataTable.Rows) {
                    if (Maths.IsEmpty(dr["SYNERGY_ID"])) {
                        dr["INNER_SETTLEMENT_CLOSE"] = "0";// '0.不需要结算'
                        dr["SYNERGY_TYPE"] = 0;//协同关系类型
                        dr["SYNERGY_D_ID"] = Maths.GuidDefaultValue();//协同序号ID
                    }
                    else {
                        dr["INNER_SETTLEMENT_CLOSE"] = "1"; //'1.需要结算'

                        #region 计算 协同关系类型
                        DependencyObjectCollection collSalesSynery = QuerySalesSynery(dr["SYNERGY_ID"]);
                        DependencyObjectCollection collSupplySynery = QuerySupplySynery(dr["SYNERGY_ID"]);
                        if (collSalesSynery.Count > 0) {
                            dr["SYNERGY_TYPE"] = 1;//1.集团销售
                        }
                        else if (collSupplySynery.Count > 0) {
                            string supplyType = collSupplySynery[0]["SUPPLY_TYPE"].ToStringExtension();
                            if (supplyType == "1") {
                                dr["SYNERGY_TYPE"] = 2;//2.集团采购
                            }
                            else if (supplyType == "2") {
                                dr["SYNERGY_TYPE"] = 3;//3.内部采购
                            }
                            else if (supplyType == "3") {
                                dr["SYNERGY_TYPE"] = 4;//4.内部配送
                            }
                        }
                        #endregion

                        #region 计算 协同序号ID
                        DependencyObjectCollection collSalesSyneryFiD = QuerySalesSyneryFiD(dr["SYNERGY_ID"]);
                        if (collSalesSyneryFiD.Count > 0) {
                            dr["SYNERGY_D_ID"] = collSalesSyneryFiD[0]["SALES_SYNERGY_FI_D_ID"];//协同序号ID
                        }
                        else {
                            dr["SYNERGY_D_ID"] = Maths.GuidDefaultValue();//协同序号ID
                        }
                        #endregion
                    }
                }

                #region 删除执行批量插入时多余列（计算列做计算时需要用的的列）

                var calcColumn = new Collection<string>();
                calcColumn.Add("");

                foreach (var column in calcColumn) {
                    if (dataTable.Columns.Contains(column)) {
                        dataTable.Columns.Remove(column); //删除执行批量插入时多余列
                    }
                }
                #endregion

                //获取需要拆入数据表的ColumnMappings
                Collection<BulkCopyColumnMapping> mapping = GetBulkCopyColumnMapping(dataTable.Columns);
                _querySrv.BulkCopy(dataTable, dataTable.TableName, mapping.ToArray()); //批量插入数据库表中
            }
        }

        /// <summary>
        /// 插入条码交易明细
        /// </summary>
        private void InsertBcLine() {
            #region properties 查询列
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                Formulas.NewId("BC_LINE_ID"),//
                OOQL.CreateProperty("TEMP_SCAN_DETAIL.Table_scan_detail_ID", "SOURCE_ID_ROid"),//
                OOQL.CreateConstants("SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_D","SOURCE_ID_RTK"),
                OOQL.CreateProperty("TEMP_SCAN_DETAIL.barcode_no", "BARCODE_NO"),//
                Formulas.Ext("UNIT_CONVERT", "QTY",
                                         new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                      Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())),
                                                      OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                                      OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                      OOQL.CreateConstants(0)
                                                      }),//库存数量 //
                Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),//
                Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID")//
                //20170330 add by wangrm for P001-170328001=====start=======
                , Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_RECEIPT.Owner_Org.RTK"),OOQL.CreateConstants(string.Empty), "Owner_Org_RTK")
                , Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_RECEIPT.Owner_Org.ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org_ROid")
                , Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_DOC_ID")
                , Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_RECEIPT.DOC_DATE"),OOQL.CreateConstants(OrmDataOption.EmptyDateTime), "DOC_DATE")
                //20170330 add by wangrm for P001-170328001=====end=======
            });
            #endregion

            QueryNode groupNode = GroupNode(true); //子查询的节点

            _queryNode =
                OOQL.Select(properties)
                    .From(groupNode, "TEMP_SCAN_DETAIL")
                    .InnerJoin(_TEMP_SCAN.Name, "TEMP_SCAN")
                    .On(OOQL.CreateProperty("TEMP_SCAN_DETAIL.info_lot_no") ==
                        OOQL.CreateProperty("TEMP_SCAN.info_lot_no"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateProperty("TEMP_SCAN.site_no"))
                    .InnerJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_no"))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_unit_no"))
                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") ==
                         OOQL.CreateProperty("TEMP_SCAN_DETAIL.warehouse_no")) &
                        (OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On((OOQL.CreateProperty("BIN.BIN_CODE") ==
                         OOQL.CreateProperty("TEMP_SCAN_DETAIL.storage_spaces_no")) &
                        (OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID")))
                    //20170330 add by wangrm for P001-170328001=====start=======
                    .LeftJoin("SALES_RETURN_RECEIPT", "SALES_RETURN_RECEIPT")
                    .On(OOQL.CreateProperty("TEMP_SCAN.Table_scan_ID") == OOQL.CreateProperty("SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_ID"))
                    //20170330 add by wangrm for P001-170328001=====end=======
                    .Where(OOQL.CreateProperty("TEMP_SCAN_DETAIL.barcode_no") != OOQL.CreateConstants(string.Empty));

            QueryNode insertNode = OOQL.Insert("BC_LINE", _queryNode, properties.Select(c => c.Alias).ToArray());
            _querySrv.ExecuteNoQuery(insertNode);
        }

        private DataTable QueryTempScan(string category, DateTime reportDatetime, object docId) {
            _queryNode =
                OOQL.Select(true,
                    OOQL.CreateProperty("TEMP_SCAN.Table_scan_ID", "SALES_RETURN_RECEIPT_ID"),
                    OOQL.CreateProperty("TEMP_SCAN.site_no", "site_no"), //用于其他列计算，插入表之前需要删除该列
                    OOQL.CreateProperty("TEMP_SCAN.info_lot_no", "info_lot_no"), //用于其他列计算，插入表之前需要删除该列
                    OOQL.CreateProperty("DOC.SEQUENCE_DIGIT", "SEQUENCE_DIGIT"), //用于其他列计算，插入表之前需要删除该列
                    Formulas.Cast(OOQL.CreateConstants(docId), GeneralDBType.Guid, "DOC_ID"),
                    Formulas.Cast(OOQL.CreateConstants(reportDatetime), GeneralDBType.Date, "DOC_DATE"),
                    Formulas.Cast(OOQL.CreateConstants(""), GeneralDBType.String, 40, "DOC_NO"), //需要计算
                    Formulas.Cast(OOQL.CreateConstants(reportDatetime), GeneralDBType.Date, "TRANSACTION_DATE"),
                    OOQL.CreateConstants(category, "CATEGORY"),
                    OOQL.CreateConstants("PLANT", "Owner_Org_RTK"),
                    Formulas.IsNull(OOQL.CreateProperty("PLANT.PLANT_ID"),
                        OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org_ROid"),
                    Formulas.IsNull(OOQL.CreateProperty("PLANT.COMPANY_ID"),
                        OOQL.CreateConstants(Maths.GuidDefaultValue()), "COMPANY_ID"),
                    Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"),
                        OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Emp"),
                    Formulas.IsNull(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"),
                        OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Dept"),
                    Formulas.Cast(OOQL.CreateConstants(0.0), GeneralDBType.Decimal, "SUM_BUSINESS_QTY"), //需要计算
                    Formulas.Cast(OOQL.CreateConstants(""), GeneralDBType.String, 40, "VIEW_DOC_NO"), //需要计算
                    OOQL.CreateConstants("", "REMARK"),
                    Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Int32, "PIECES"),
                    Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN.CUSTOMER_ID"),
                        OOQL.CreateConstants(Maths.GuidDefaultValue()), "SHIP_TO_CUSTOMER_ID"),
                //20170619 add by zhangcn for P001-170606002 ===begin===
                    Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN.ALL_SYNERGY"), OOQL.CreateConstants(false), "ALL_SYNERGY"),
                    Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN.GROUP_SYNERGY_ID.ROid"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "GROUP_SYNERGY_ID_ROid"),
                    Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN.GROUP_SYNERGY_ID.RTK"), OOQL.CreateConstants(string.Empty), "GROUP_SYNERGY_ID_RTK"),
                    Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN.SOURCE_CUSTOMER_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_CUSTOMER_ID"),
                    OOQL.CreateConstants("", "GENERATE_NO"),
                    OOQL.CreateConstants(false, "GENERATE_STATUS"),
                    OOQL.CreateConstants(0, "DOC_Sequence"),//需要计算
                    Formulas.Cast(OOQL.CreateConstants(Maths.GuidDefaultValue()), GeneralDBType.Guid, "GROUP_SYNERGY_D_ID"),//需要计算
                    //20170619 add by zhangcn for P001-170606002 ===end===
                    Formulas.Cast(OOQL.CreateConstants(Maths.GuidDefaultValue()), GeneralDBType.Guid, "WAREHOUSE_ID"),
                    Formulas.Cast(OOQL.CreateConstants(1), GeneralDBType.Int32, "STOCK_ACTION"),
                    Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Boolean, "SOURCE_UNCONFIRM"),
                    OOQL.CreateConstants(1, "ACCOUNT_YEAR"),
                    Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Int32, "ACCOUNT_PERIOD_SEQNO"),
                    Formulas.Cast(OOQL.CreateConstants(""), GeneralDBType.String, "ACCOUNT_PERIOD_CODE"),
                    OOQL.CreateConstants("N", "ApproveStatus"),
                    Formulas.GetDate("CreateDate"))
                    .From(_TEMP_SCAN.Name, "TEMP_SCAN")
                    .LeftJoin("EMPLOYEE", "EMPLOYEE")
                    .On(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE") == OOQL.CreateProperty("TEMP_SCAN.employee_no"))
                    .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                    .On(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE") ==
                        OOQL.CreateProperty("TEMP_SCAN.picking_department_no"))
                    .LeftJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateProperty("TEMP_SCAN.site_no"))
                    .LeftJoin("SALES_RETURN", "SALES_RETURN")
                    .On(OOQL.CreateProperty("SALES_RETURN.DOC_NO") == OOQL.CreateProperty("TEMP_SCAN.info_lot_no"))
                    .LeftJoin("PARA_DOC_FIL", "PARA_DOC_FIL")
                    .On(OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID") &
                        OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants(category))
                    .LeftJoin("DOC", "DOC")
                    .On(OOQL.CreateProperty("DOC.DOC_ID") == OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID"));
                    //.LeftJoin("DOC.DOC_D", "DOC_D")
                    //.On(OOQL.CreateProperty("DOC_D.DOC_ID") == OOQL.CreateProperty("DOC.DOC_ID") &
                    //    OOQL.CreateProperty("DOC_D.ORG_ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"));

            return _querySrv.Execute(_queryNode);
        }


        private DataTable QueryTempScanDetail() {
            #region properties 查询列
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                            OOQL.CreateProperty("TEMP_SCAN.Table_scan_ID", "ParentId"),//父主键   SALES_RETURN_RECEIPT_ID
                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.Table_scan_detail_ID", "SALES_RETURN_RECEIPT_D_ID"),//
                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.SequenceNumber", "SequenceNumber"),//
                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty", "BUSINESS_QTY"),//
                            OOQL.CreateProperty("ITEM.ITEM_ID", "ITEM_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.ITEM_DESCRIPTION"),OOQL.CreateConstants(string.Empty), "ITEM_DESCRIPTION"),//
                            Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.ITEM_SPECIFICATION"),OOQL.CreateConstants(string.Empty), "ITEM_SPECIFICATION"),//
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "ITEM_FEATURE_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "BUSINESS_UNIT_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "WAREHOUSE_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "BIN_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "ITEM_LOT_ID"),//
                            Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY",
                                         new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                      Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid)),
                                                      OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                                      OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                      OOQL.CreateConstants(0)
                                                      }),//库存数量 //
                           Formulas.Ext("UNIT_CONVERT", "SECOND_QTY",
                                         new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                      Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid)),
                                                      OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                                      OOQL.CreateProperty("ITEM.SECOND_UNIT_ID"),
                                                      OOQL.CreateConstants(0)
                                                      }),//第二数量 //
                           OOQL.CreateConstants(0,GeneralDBType.Int32,"PIECES"),//
                           OOQL.CreateConstants(OrmDataOption.EmptyDateTime,GeneralDBType.DateTime,"PLAN_SETTLEMENT_DATE"),//
                           OOQL.CreateConstants(string.Empty,"REMARK"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"PACKING_QTY1"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"PACKING_QTY2"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"PACKING_QTY3"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"PACKING_QTY4"),//
                           OOQL.CreateConstants(string.Empty,"PACKING_QTY"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"AMOUNT_UNINCLUDE_TAX_OC"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"AMOUNT_UNINCLUDE_TAX_BC"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"TAX_OC"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"TAX_BC"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"SETTLEMENT_PRICE_QTY"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"SETTLEMENT_BUSINESS_QTY"),//
                           OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid,"PACKING1_UNIT_ID"),//
                           OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid,"PACKING2_UNIT_ID"),//
                           OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid,"PACKING3_UNIT_ID"),//
                           OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid,"PACKING4_UNIT_ID"),//
                           OOQL.CreateConstants(string.Empty,"INNER_SETTLEMENT_CLOSE"),//需要计算
                           Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.ITEM_TYPE"),OOQL.CreateConstants(string.Empty), "ITEM_TYPE"),//
                           Formulas.Case(null,
                                         OOQL.CreateConstants("1"),//‘1. 结算未完成’
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CHARACTER") == OOQL.CreateConstants("2") |
                                                                                   OOQL.CreateProperty("SALES_RETURN_TEMP.ITEM_TYPE").In(OOQL.CreateConstants("2"),OOQL.CreateConstants("3"))), 
                                                                                  OOQL.CreateConstants("0"))),//‘0.不需结算’
                                         "SETTLEMENT_CLOSE"), //结算状态//
                           OOQL.CreateConstants("OTHER","SYNERGY_SOURCE_ID_RTK"),//
                           OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid,"SYNERGY_SOURCE_ID_ROid"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"PRICE_QTY"),//
                           Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.PRICE_UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "PRICE_UNIT_ID"),//
                           OOQL.CreateConstants("SALES_RETURN.SALES_RETURN_D","SOURCE_ID_RTK"),//
                           Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SALES_RETURN_D_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "SOURCE_ID_ROid"),//
                           Formulas.Case(null,
                                         OOQL.CreateConstants("1"),//‘1.未完成’
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT") == OOQL.CreateConstants(false,GeneralDBType.Boolean) |
                                                                                   OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty") == OOQL.CreateConstants(0,GeneralDBType.Decimal)), 
                                                                                  OOQL.CreateConstants("0"))),//‘0.无需检核’
                                         "SN_COLLECTED_STATUS"), //序列号检核码//
                           Formulas.Case(null,
                                         OOQL.CreateConstants("OTHER"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_RTK") == OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD"),
                                                                                  OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD")),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_RTK") == OOQL.CreateConstants("SALES_DELIVERY.SALES_DELIVERY_D"),
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SALES_DELIVERY_D_SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty))),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_RTK") == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D"),
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SALES_ISSUE_D_ORDER_SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty))),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_RTK") == OOQL.CreateConstants("PURCHASE_ISSUE.PURCHASE_ISSUE_D"),
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.PURCHASE_ISSUE_D_ORDER_SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)))
                                                             ),
                                         "ORDER_SOURCE_ID_RTK"), //订单来源类型//
                           Formulas.Case(null,
                                         OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_RTK") == OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD"), 
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid))),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_RTK") == OOQL.CreateConstants("SALES_DELIVERY.SALES_DELIVERY_D") &
                                                                                  OOQL.CreateProperty("SALES_RETURN_TEMP.SALES_DELIVERY_D_SOURCE_ID_RTK").In(OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD"),
                                                                                                                                                             OOQL.CreateConstants("INNER_ORDER_DOC.INNER_ORDER_DOC_D.INNER_ORDER_DOC_SD")), 
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SALES_DELIVERY_D_SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid))),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_RTK") == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D"), 
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SALES_ISSUE_D_ORDER_SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid))),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ID_RTK") == OOQL.CreateConstants("PURCHASE_ISSUE.PURCHASE_ISSUE_D"), 
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.PURCHASE_ISSUE_D_ORDER_SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid)))
                                                             ),
                                         "ORDER_SOURCE_ID_ROid"), //订单来源//
                            Formulas.Case(null,
                                         OOQL.CreateConstants("COST_DOMAIN"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants(1))), 
                                                                                    OOQL.CreateConstants("COMPANY"))),
                                         "COST_DOMAIN_ID_RTK"), //成本域类型//
                           Formulas.Case(null,
                                         OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants(1),
                                                                                  OOQL.CreateProperty("PLANT.COMPANY_ID")),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants(2),
                                                                                  OOQL.CreateProperty("PLANT.COST_DOMAIN_ID")),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants(3),
                                                                                  Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.COST_DOMAIN_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid)))),
                                         "COST_DOMAIN_ID_ROid"), //成本域//
                           Formulas.Case(null,
                                         OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.INTERNAL") == OOQL.CreateConstants(true,GeneralDBType.Boolean),
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SYNERGY_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid))),
                                                              OOQL.CreateCaseItem(OOQL.CreateProperty("SALES_RETURN_TEMP.ORDER_SOURCE_ID") != OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid) &
                                                                                  OOQL.CreateProperty("SALES_RETURN_TEMP.DELIVERY_PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID") & //单头的Owner_Org.ROid
                                                                                  OOQL.CreateProperty("SALES_RETURN_TEMP.SYNERGY_SOURCE_ID_RTK") == OOQL.CreateConstants("SALES_SYNERGY"),
                                                                                  Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SYNERGY_SOURCE_ID_ROid"), OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid)))
                                                              ),
                                         "SYNERGY_ID"), //协同关系
                           OOQL.CreateConstants(0,GeneralDBType.Int32,"SYNERGY_TYPE"),//需要计算
                           OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid,"SYNERGY_D_ID"),//需要计算
                           OOQL.CreateConstants(0,GeneralDBType.Int32,"SETTLEMENT_PATH_TYPE"),//
                           OOQL.CreateConstants(0,GeneralDBType.Int32,"RE_SETTLEMENT_PATH_TYPE"),//
                           OOQL.CreateConstants(true,GeneralDBType.Boolean,"SETTLEMENT_START_INDICATOR"),//
                           OOQL.CreateConstants(OrmDataOption.EmptyDateTime,"ESTI_PAYMENT_DATE"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"SETTLEMENT_AMT_UN_TAX_OC"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"SETTLEMENT_TAX_OC"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"SETTLEMENT_AMT_UN_TAX_BC"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"SETTLEMENT_TAX_BC"),//
                           OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid,"PROJECT_ID"),//
                           OOQL.CreateConstants(0,GeneralDBType.Decimal,"SN_COLLECTED_QTY"),//
                           //20170619 add by zhangcn for P001-170606002 ===begin===
                           Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ORDER_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "SOURCE_ORDER_ROid"),
                           Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.SOURCE_ORDER_RTK"),OOQL.CreateConstants(string.Empty), "SOURCE_ORDER_RTK"),
                           OOQL.CreateConstants(0,GeneralDBType.Int32,"SYNERGY_SETTLEMENT_GROUP"),
                           //20170619 add by zhangcn for P001-170606002 ===end===
                           Formulas.IsNull(OOQL.CreateProperty("SALES_RETURN_TEMP.PACKING_MODE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "PACKING_MODE_ID"),//销退单.包装方式//
                           OOQL.CreateConstants("N", "ApproveStatus")
            });
            #endregion

            QueryNode groupNode = GroupNode(false); //子查询的节点
            QueryNode salesReturnQueryNode = GetSalesReturnQueryNode();
            _queryNode =
                OOQL.Select(properties)
                    .From(groupNode, "TEMP_SCAN_DETAIL")
                    .InnerJoin(_TEMP_SCAN.Name, "TEMP_SCAN")
                    .On(OOQL.CreateProperty("TEMP_SCAN_DETAIL.info_lot_no") == OOQL.CreateProperty("TEMP_SCAN.info_lot_no"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateProperty("TEMP_SCAN.site_no"))
                    .InnerJoin("PARA_COMPANY", "PARA_COMPANY")
                    .On(OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.COMPANY_ID"))
                    .InnerJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_no"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no") &
                        OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_unit_no"))
                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                    .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.warehouse_no") &
                        OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On(OOQL.CreateProperty("BIN.BIN_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.storage_spaces_no") &
                        OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                    .LeftJoin(salesReturnQueryNode, "SALES_RETURN_TEMP")
                    .On(OOQL.CreateProperty("SALES_RETURN_TEMP.DOC_NO") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.doc_no") &
                        OOQL.CreateProperty("SALES_RETURN_TEMP.SequenceNumber") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.seq"))
                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                    .On(OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.lot_no") &
                        OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID") &
                        ((OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no") == OOQL.CreateConstants(string.Empty) &
                          OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue())) |
                         (OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no") != OOQL.CreateConstants(string.Empty) &
                          OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())))))
                        ;

            //3.0平台BulkCopy()方法可能不支持 ParentId，所以先插入，然后再查询出来修改
            QueryNode insertNode = OOQL.Insert("SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_D", _queryNode, properties.Select(c => c.Alias).ToArray());
            _querySrv.ExecuteNoQuery(insertNode);

            return _querySrv.Execute(_queryNode);
        }

        /// <summary>
        /// 校验单据类型
        /// </summary>
        private object ValidateParaFilDoc(DependencyObjectCollection collScan, string category) {
            object sourceDocId = Maths.GuidDefaultValue();
            
            string infoLotNo = collScan[0]["info_lot_no"].ToStringExtension();
            string siteNo = collScan[0]["site_no"].ToStringExtension();

            _queryNode =
                OOQL.Select(OOQL.CreateProperty("SALES_RETURN.DOC_ID", "DOC_ID"))
                    .From("SALES_RETURN", "SALES_RETURN")
                    .Where(OOQL.CreateProperty("SALES_RETURN.DOC_NO") == OOQL.CreateConstants(infoLotNo));
            DependencyObjectCollection collSalesReturn = _querySrv.ExecuteDependencyObject(_queryNode);
            if (collSalesReturn.Count > 0){
                sourceDocId = collSalesReturn[0]["DOC_ID"];
            }

            _queryNode =
               OOQL.Select(1, OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID", "DOC_ID"))
                   .From("PARA_DOC_FIL", "PARA_DOC_FIL")
                   .InnerJoin("PLANT", "PLANT")
                   .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid"))
                   .Where(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo) &
                          OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants(category) &
                          (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(sourceDocId) |
                           OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                   ).OrderBy(new OrderByItem(OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"), SortType.Desc));

            DependencyObjectCollection collParaDocFil = _querySrv.ExecuteDependencyObject(_queryNode);
            if (collParaDocFil.Count == 0){
                throw new BusinessRuleException(GetService<IInfoEncodeContainer>().GetMessage("A111275"));
            }

            return collParaDocFil[0]["DOC_ID"];
        }

        #endregion

        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="isEntityLine"></param>
        /// <returns></returns>
        public QueryNode GroupNode(bool isEntityLine) {
            List<QueryProperty> properties = new List<QueryProperty>{
                OOQL.CreateProperty("TEMP_SCAN_DETAIL.Table_scan_detail_ID")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.site_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.info_lot_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.warehouse_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.storage_spaces_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.lot_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_unit_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.doc_no")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.seq")
                ,OOQL.CreateProperty("TEMP_SCAN_DETAIL.SequenceNumber")
            };

            List<QueryProperty> groupProperties = new List<QueryProperty>();
            groupProperties = new List<QueryProperty>();
            groupProperties.AddRange(properties);

            if (isEntityLine) {
                properties.Add(OOQL.CreateProperty("TEMP_SCAN_DETAIL.barcode_no"));
                groupProperties.Add(OOQL.CreateProperty("TEMP_SCAN_DETAIL.barcode_no"));
            }

            properties.Add(Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty")), OOQL.CreateConstants(0, GeneralDBType.Decimal),
                "picking_qty"));

            QueryNode node = OOQL.Select(properties)
                .From(_TEMP_SCAN_DETAIL.Name, "TEMP_SCAN_DETAIL")
                .GroupBy(groupProperties);

            return node;
        }

        /// <summary>
        /// 查询销退入库单，用于自动签核
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection QuerySalesReturnReceipt() {
            _queryNode =
                OOQL.Select(OOQL.CreateProperty("SALES_RETURN_RECEIPT.DOC_ID", "DOC_ID"),
                            OOQL.CreateProperty("SALES_RETURN_RECEIPT.DOC_NO", "DOC_NO"),
                            OOQL.CreateProperty("SALES_RETURN_RECEIPT.Owner_Org.ROid", "Owner_Org_ROid"),
                            OOQL.CreateProperty("SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_ID", "SALES_RETURN_RECEIPT_ID"))
                    .From(_TEMP_SCAN.Name, "TEMP_SCAN")
                    .InnerJoin("SALES_RETURN_RECEIPT", "SALES_RETURN_RECEIPT")
                    .On(OOQL.CreateProperty("SALES_RETURN_RECEIPT.SALES_RETURN_RECEIPT_ID") ==
                        OOQL.CreateProperty("TEMP_SCAN.Table_scan_ID"));

            return _querySrv.ExecuteDependencyObject(_queryNode);
        }

        /// <summary>
        /// 查询FIL参数
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection QueryParaFil() {
            _queryNode =
                OOQL.Select(OOQL.CreateProperty("PARA_FIL.BC_LINE_MANAGEMENT", "BC_LINE_MANAGEMENT"))
                    .From("PARA_FIL", "PARA_FIL");

            return _querySrv.ExecuteDependencyObject(_queryNode);
        }

        private QueryNode GetSalesReturnQueryNode() {
            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("SALES_RETURN.DOC_NO", "DOC_NO"),
                            OOQL.CreateProperty("SALES_RETURN_D.SequenceNumber", "SequenceNumber"),
                            OOQL.CreateProperty("SALES_RETURN_D.ITEM_DESCRIPTION", "ITEM_DESCRIPTION"),
                            OOQL.CreateProperty("SALES_RETURN_D.ITEM_SPECIFICATION", "ITEM_SPECIFICATION"),
                            OOQL.CreateProperty("SALES_RETURN_D.ITEM_TYPE", "ITEM_TYPE"),
                            OOQL.CreateProperty("SALES_RETURN_D.PRICE_UNIT_ID", "PRICE_UNIT_ID"),
                            OOQL.CreateProperty("SALES_RETURN_D.SALES_RETURN_D_ID", "SALES_RETURN_D_ID"),
                            OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID.RTK", "SOURCE_ID_RTK"),
                            OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID.ROid", "SOURCE_ID_ROid"),//SALES_RETURN_TEMP
                            //OOQL.CreateProperty("SALES_RETURN_D.REFERENCE_SOURCE_ID.RTK", "REFERENCE_SOURCE_ID_RTK"),
                            //OOQL.CreateProperty("SALES_RETURN_D.REFERENCE_SOURCE_ID.ROid", "REFERENCE_SOURCE_ID_ROid"),
                            OOQL.CreateProperty("SALES_RETURN_D.ORDER_SOURCE_ID", "ORDER_SOURCE_ID"),
                            OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK", "SALES_DELIVERY_D_SOURCE_ID_RTK"),
                            OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.ROid", "SALES_DELIVERY_D_SOURCE_ID_ROid"),
                            OOQL.CreateProperty("SALES_ISSUE_D.ORDER_SOURCE_ID.RTK", "SALES_ISSUE_D_ORDER_SOURCE_ID_RTK"),
                            OOQL.CreateProperty("SALES_ISSUE_D.ORDER_SOURCE_ID.ROid", "SALES_ISSUE_D_ORDER_SOURCE_ID_ROid"),
                            OOQL.CreateProperty("PURCHASE_ISSUE_D.ORDER_SOURCE_ID.RTK", "PURCHASE_ISSUE_D_ORDER_SOURCE_ID_RTK"),
                            OOQL.CreateProperty("PURCHASE_ISSUE_D.ORDER_SOURCE_ID.ROid", "PURCHASE_ISSUE_D_ORDER_SOURCE_ID_ROid"),
                            OOQL.CreateProperty("SALES_ORDER_DOC_SD.DELIVERY_PLANT_ID", "DELIVERY_PLANT_ID"),
                            OOQL.CreateProperty("DOC.INTERNAL", "INTERNAL"),
                            OOQL.CreateProperty("SALES_RETURN.SYNERGY_ID", "SYNERGY_ID"),
                            OOQL.CreateProperty("SALES_RETURN.SYNERGY_D_ID", "SYNERGY_D_ID"),
                            OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID_ROid", "SYNERGY_SOURCE_ID_ROid"),
                            OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID_RTK", "SYNERGY_SOURCE_ID_RTK"),
                            //OOQL.CreateProperty("SALES_ORDER_DOC_SD_RE.SYNERGY_SOURCE_ID_ROid", "SYNERGY_SOURCE_ID_ROid_RE"),
                            OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ORDER.ROid", "SOURCE_ORDER_ROid"),//20170619 add by zhangcn for P001-170606002
                            OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ORDER.RTK", "SOURCE_ORDER_RTK"),//20170619 add by zhangcn for P001-170606002
                            OOQL.CreateProperty("SALES_RETURN_D.PACKING_MODE_ID", "PACKING_MODE_ID"))
                    .From("SALES_RETURN", "SALES_RETURN")
                    .InnerJoin("SALES_RETURN.SALES_RETURN_D", "SALES_RETURN_D")
                    .On(OOQL.CreateProperty("SALES_RETURN_D.SALES_RETURN_ID") == OOQL.CreateProperty("SALES_RETURN.SALES_RETURN_ID"))
                    .InnerJoin("DOC", "DOC")
                    .On(OOQL.CreateProperty("DOC.DOC_ID") == OOQL.CreateProperty("SALES_RETURN.DOC_ID"))
                    .LeftJoin("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D")
                    .On(OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID.ROid") == OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_D_ID") &
                        OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID.RTK") == OOQL.CreateConstants("SALES_DELIVERY.SALES_DELIVERY_D"))
                    .LeftJoin("SALES_ISSUE.SALES_ISSUE_D", "SALES_ISSUE_D")
                    .On(OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID_ROid") == OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_D_ID") &
                        OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID.RTK") == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D"))
                    .LeftJoin("PURCHASE_ISSUE.PURCHASE_ISSUE_D", "PURCHASE_ISSUE_D")
                    .On(OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID_ROid") == OOQL.CreateProperty("PURCHASE_ISSUE_D.PURCHASE_ISSUE_D_ID") &
                        OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID.RTK") == OOQL.CreateConstants("PURCHASE_ISSUE.PURCHASE_ISSUE_D"))
                    .LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD")
                    .On(OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_SD_ID") == OOQL.CreateProperty("SALES_RETURN_D.ORDER_SOURCE_ID")) 

                    //.LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD")
                    //.On(OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_SD_ID") == OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID.ROid") &
                    //    OOQL.CreateProperty("SALES_RETURN_D.SOURCE_ID.RTK") == OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD"))
                    //.LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD_RE")
                    //.On(OOQL.CreateProperty("SALES_ORDER_DOC_SD_RE.SALES_ORDER_DOC_SD_ID") == OOQL.CreateProperty("SALES_RETURN_D.REFERENCE_SOURCE_ID.ROid") &
                    //    OOQL.CreateProperty("SALES_RETURN_D.REFERENCE_SOURCE_ID.RTK") == OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD"))
                    ;

            return queryNode;
        }

        private DependencyObjectCollection QuerySalesSynery(object syneryId) {
            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("SALES_SYNERGY.SALES_SYNERGY_ID", "SALES_SYNERGY_ID"))
                    .From("SALES_SYNERGY", "SALES_SYNERGY")
                    .Where(OOQL.CreateProperty("SALES_SYNERGY.SALES_SYNERGY_ID") == OOQL.CreateConstants(syneryId));

            return _querySrv.ExecuteDependencyObject(queryNode);
        }

        private DependencyObjectCollection QuerySupplySynery(object syneryId) {
            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_TYPE", "SUPPLY_TYPE"))
                    .From("SUPPLY_SYNERGY", "SUPPLY_SYNERGY")
                    .Where(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID") == OOQL.CreateConstants(syneryId));

            return _querySrv.ExecuteDependencyObject(queryNode);
        }

        private DependencyObjectCollection QuerySalesSyneryFiD(object syneryId) {
            QueryNode queryNode =
                OOQL.Select(1, 
                            OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_FI_D_ID", "SALES_SYNERGY_FI_D_ID"),
                            OOQL.CreateProperty("SALES_SYNERGY_FI_D.SequenceNumber", "SequenceNumber"))
                    .From("SALES_SYNERGY.SALES_SYNERGY_FI_D", "SALES_SYNERGY_FI_D")
                    .Where(OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_ID") == OOQL.CreateConstants(syneryId))
                    .OrderBy(new OrderByItem(OOQL.CreateProperty("SALES_SYNERGY_FI_D.SequenceNumber"), SortType.Desc));

            return _querySrv.ExecuteDependencyObject(queryNode);
        }

        /// <summary>
        /// 创建服务返回集合
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection CreateReturnCollection() {
            DependencyObjectType type = new DependencyObjectType("ReturnCollection");
            type.RegisterSimpleProperty("doc_no", typeof(string));

            DependencyObjectCollection collDepObj = new DependencyObjectCollection(type);

            return collDepObj;
        }


        #endregion

        #region 辅助方法

        /// <summary>
        ///     通过DataTable列名转换为ColumnMappings
        /// </summary>
        /// <param name="columns">表的列的集合</param>
        /// <returns>Mapping集合</returns>
        private Collection<BulkCopyColumnMapping> GetBulkCopyColumnMapping(DataColumnCollection columns) {
            Collection<BulkCopyColumnMapping> mapping = new Collection<BulkCopyColumnMapping>();
            foreach (DataColumn column in columns) {
                //列名
                string targetName = column.ColumnName;
                //列名中的下划线大于0，且以[_RTK]或[_ROid]结尾的列名视为多来源字段
                if (targetName.IndexOf("_") > 0
                    && (targetName.EndsWith("_RTK", StringComparison.CurrentCultureIgnoreCase)
                        || targetName.EndsWith("_ROid", StringComparison.CurrentCultureIgnoreCase))) {
                    //列名长度
                    int nameLength = targetName.Length;
                    //最后一个下划线后一位位置
                    int endPos = targetName.LastIndexOf("_") + 1;
                    //拼接目标字段名
                    targetName = targetName.Substring(0, endPos - 1) + "." + targetName.Substring(endPos, nameLength - endPos);
                }
                var mappingItem = new BulkCopyColumnMapping(column.ColumnName, targetName);
                //mapping结合中不存在便添加元数
                if (!mapping.Contains(mappingItem)) {
                    mapping.Add(mappingItem);
                }
            }
            return mapping;
        }

        public class EntityLine {
            /// <summary>
            /// 决定唯一性的相关字段
            /// </summary>
            public string UniqueKey { get; set; }

            /// <summary>
            /// 行ID
            /// </summary>
            public object Key { get; set; }

            /// <summary>
            /// 行序号
            /// </summary>
            public int SequenceNumber { get; set; }
        }
        #endregion
    }
}
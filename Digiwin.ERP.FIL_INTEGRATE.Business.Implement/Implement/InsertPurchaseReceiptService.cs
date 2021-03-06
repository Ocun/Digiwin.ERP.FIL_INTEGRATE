﻿//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-08</createDate>
//<description>生成采购入库单 实现</description>
//----------------------------------------------------------------
//20161213 modi by shenbao for B001-161213006 校验单据类型
//20170209 modi by liwei1 for P001-170203001 修正单据类型取值
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
    /// 生成采购入库单
    /// </summary>
    [ServiceClass(typeof(IInsertPurchaseReceiptService))]
    [Description("生成采购入库单")]
    public class InsertPurchaseReceiptService : ServiceComponent, IInsertPurchaseReceiptService {
        #region 属性

        private IQueryService _querySrv;
        private IBusinessTypeService _businessTypeSrv;
        private QueryNode _queryNode;
        private IDataEntityType _TEMP_SCAN;
        private IDataEntityType _TEMP_SCAN_DETAIL;
        private Dictionary<object, decimal> _dicSumBusinessQty;
        private DependencyObjectCollection _collDocNos;

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
            get { return _primaryKeySrv ?? (_primaryKeySrv = GetService<IPrimaryKeyService>("PURCHASE_RECEIPT")); }
        }

        private IDocumentNumberGenerateService _documentNumberGenSrv;
        /// <summary>
        /// 生成单号服务
        /// </summary>
        public IDocumentNumberGenerateService DocumentNumberGenSrv {
            get {
                return _documentNumberGenSrv ??
                       (_documentNumberGenSrv = GetService<IDocumentNumberGenerateService>("PURCHASE_RECEIPT"));
            }
        }

        #endregion

        #region 接口方法
        /// <summary>
        /// 插入领料出库单
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
        public DependencyObjectCollection DoInsertPurchaseReceipt(string employeeNo, string scanType, DateTime reportDatetime,
            string pickingDepartmentNo, string recommendedOperations, string recommendedFunction,
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
            _collDocNos = CreateReturnCollection();
            List<object> lstPurchaseReceiptIds = new List<object>();
            string category = "12"; //采购入库
            //20170209 add by liwei1 for P001-170203001 ===begin===
            object docId = QueryDocId(category, collScan);
            if (Maths.IsEmpty(docId)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111275"));
            }
            //20170209 add by liwei1 for P001-170203001 ===end===
            DataTable dtTempScan = null;//临时表Scan的DataTable结构
            DataTable dtTempScanDetail = null;//临时表ScanDetail的DataTable结构
            List<BulkCopyColumnMapping> lstColumnMappingsScan = null;//临时表Scan的映射列
            List<BulkCopyColumnMapping> lstColumnMappingsScanDetail = null;//临时表ScanDetail的映射列
            CreateTempTableMapping(ref dtTempScan, ref lstColumnMappingsScan, ref dtTempScanDetail, ref lstColumnMappingsScanDetail);//创建临时表#Table_scan的列映射
            InsertTempTableData(dtTempScan, dtTempScanDetail, collScan, employeeNo, pickingDepartmentNo);//插入DataTable数据
            #endregion

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

                    #region  7.2生成采购入库实体，调用平台的保存服务生成单据(自动审核的单据平台会自动审核)
                    InsertPurchaseReceipt(category, reportDatetime, lstPurchaseReceiptIds, docId);//7.2.1 插入采购入库单(PURCHASE_RECEIPT)  //20170209 add by liwei1 for P001-170203001 增加参数:docId
                    InsertPurchaseReceiptD();//7.2.2 插入采购入库单单身(PURCHASE_RECEIPT_D)

                    DependencyObjectCollection collParaFil = QueryParaFil();
                    if (collParaFil.Count > 0 && collParaFil[0]["BC_LINE_MANAGEMENT"].ToBoolean()) {
                        InsertBcLine();//插入条码交易明细 BC_LINE
                    }

                    #endregion

                    //7.3自动签核
                    DependencyObjectCollection collPurchase = QueryPurchase();
                    string pProgramInfo = "PURCHASE_RECEIPT.I01";
                    if (collPurchase.Count > 0) {
                        IEFNETStatusStatusService efnetSrv = this.GetService<IEFNETStatusStatusService>();
                        foreach (DependencyObject objPurchase in collPurchase) {
                            efnetSrv.GetFormFlow(pProgramInfo, objPurchase["DOC_ID"], objPurchase["Owner_Org_ROid"],
                                 new List<object>() { objPurchase["PURCHASE_RECEIPT_ID"] });
                        }
                    }

                }

                //保存单据
                IReadService readSrv = GetService<IReadService>("PURCHASE_RECEIPT");
                object[] entities = readSrv.Read(lstPurchaseReceiptIds.ToArray());
                if (entities != null && entities.Length > 0) {
                    ISaveService saveSrv = this.GetService<ISaveService>("PURCHASE_RECEIPT");
                    saveSrv.Save(entities);
                }

                transService.Complete(); //事务提交
            }

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
                                               null, false,new Attribute[] { _businessTypeSrv.SimplePrimaryKey });
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
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });

            //工厂 
            defaultType.RegisterSimpleProperty("site_no", _businessTypeSrv.SimpleFactoryType,
                                               string.Empty, false, new Attribute[] { _businessTypeSrv.SimpleFactory });

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
                    "info_lot_no",  //信息批号
                    "site_no",  //信息批号
                    "barcode_no",  
                    "item_no",
                    "item_feature_no",                   
                    "warehouse_no",                   
                    "storage_spaces_no",                   
                    "lot_no",                    
                    "picking_qty",                    
                    "picking_unit_no",                   
                    "doc_no",                    
                    "seq"  ,                
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
                        string uniqueKey = string.Concat(objScanDetail["info_lot_no"].ToStringExtension(), objScanDetail["doc_no"].ToStringExtension(), objScanDetail["seq"].ToStringExtension()
                        , objScanDetail["item_no"].ToStringExtension(), objScanDetail["item_feature_no"].ToStringExtension(), objScanDetail["warehouse_no"].ToStringExtension()
                        , objScanDetail["storage_spaces_no"].ToStringExtension(), objScanDetail["lot_no"].ToStringExtension(), objScanDetail["picking_unit_no"].ToStringExtension()
                        , objScanDetail["site_no"].ToStringExtension());

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
                        drScanDetail["info_lot_no"] = objScanDetail["info_lot_no"];
                        drScanDetail["site_no"] = objScanDetail["site_no"];
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
                        else{
                            _dicSumBusinessQty[drScanDetail["info_lot_no"]] = drScanDetail["picking_qty"].ToDecimal() +
                                                                              _dicSumBusinessQty[drScanDetail["info_lot_no"]];
                        }
                    }
                }
                #endregion
            }
        }

        #endregion

        #region 插入操作

        /// <summary>
        /// 插入采购入库单
        /// </summary>
        /// <param name="category">种类</param>
        /// <param name="reportDatetime">单据日期</param>
        /// <param name="lstPurchaseReceiptIds"></param>
        private void InsertPurchaseReceipt(string category, DateTime reportDatetime, List<object> lstPurchaseReceiptIds,object docId) {//20170209 modi by liwei1 for P001-170203001 增加参数:docId
            DataTable dataTable = QueryTempScan(category, reportDatetime, docId);//20170209 modi by liwei1 for P001-170203001 增加参数:docId
            ValidateParaFilDoc(dataTable);  //20161213 add by shenbao for B001-161213006
            dataTable.TableName = "PURCHASE_RECEIPT";
            if (dataTable.Rows.Count > 0) {
                string tempDocNo = string.Empty; //临时变量，用于产生新的采购退货单单号
                Dictionary<string, string> dicDocNo = new Dictionary<string, string>(); //生成采购入库单单号字典

                foreach (DataRow dr in dataTable.Rows) {
                    lstPurchaseReceiptIds.Add(dr["PURCHASE_RECEIPT_ID"]);
                   
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
                        DependencyObjectCollection collSalesSyneryFiD = QuerySupplySyneryFiD(dr["GROUP_SYNERGY_ID_ROid"]);
                        if (collSalesSyneryFiD.Count > 0){
                            dr["GROUP_SYNERGY_D_ID"] = collSalesSyneryFiD[0]["SUPPLY_SYNERGY_FI_D_ID"];
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

                foreach (var column in calcColumn){
                    if (dataTable.Columns.Contains(column)){
                        dataTable.Columns.Remove(column); //删除执行批量插入时多余列
                    }
                }
                #endregion

                //获取需要拆入数据表的ColumnMappings
                var mapping = GetBulkCopyColumnMapping(dataTable.Columns);
                _querySrv.BulkCopy(dataTable, dataTable.TableName, mapping.ToArray()); //批量插入数据库表中
            }
        }

        //20161213 add by shenbao for B001-161213006
        /// <summary>
        /// 校验单据类型
        /// </summary>
        /// <param name="dt"></param>
        private void ValidateParaFilDoc(DataTable dt) {
            foreach (DataRow dr in dt.Rows) {
                if (Maths.IsEmpty(dr["DOC_ID"])) {
                    throw new BusinessRuleException(this.GetService<IInfoEncodeContainer>().GetMessage("A111275"));
                }
            }
        }

        /// <summary>
        /// 插入采购入库单单身
        /// </summary>
        private void InsertPurchaseReceiptD() {
            QueryNode purchaseArrivalTemp = QueryPurchaseArrival();

            #region properties 查询列 
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.Table_scan_detail_ID", "PURCHASE_RECEIPT_D_ID"),//
                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty", "BUSINESS_QTY"),//
                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.SequenceNumber", "SequenceNumber"),//
                            OOQL.CreateProperty("TEMP_SCAN.Table_scan_ID", "PURCHASE_RECEIPT_ID"),//
                            OOQL.CreateProperty("ITEM.ITEM_ID", "ITEM_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "ITEM_FEATURE_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BUSINESS_UNIT_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "ITEM_LOT_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.PURCHASE_ARRIVAL_D_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.ITEM_TYPE"),OOQL.CreateConstants(string.Empty), "ITEM_TYPE"),//
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.PURCHASE_TYPE"),OOQL.CreateConstants(string.Empty), "PURCHASE_TYPE"),//
                            OOQL.CreateConstants(0, GeneralDBType.Int32, "PIECES"),//
                            OOQL.CreateConstants(OrmDataOption.EmptyDateTime, GeneralDBType.DateTime, "PLAN_SETTLEMENT_DATE"),//
                            OOQL.CreateConstants(string.Empty, "REMARK"),//
                            OOQL.CreateConstants(0, GeneralDBType.Int32, "ESTIMATION_TIMES"),//
                            OOQL.CreateConstants(0m, GeneralDBType.Decimal, "PRICE_QTY"),//

                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.PRICE_UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "PRICE_UNIT_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.PACKING_MODE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "PACKING_MODE_ID"),//
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.ITEM_DESCRIPTION"),OOQL.CreateConstants(string.Empty), "ITEM_DESCRIPTION"),//
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.ITEM_SPECIFICATION"),OOQL.CreateConstants(string.Empty), "ITEM_SPECIFICATION"),//
                            Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY",
                                         new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                       Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())),
                                                      OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                                      OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                      OOQL.CreateConstants(0)
                                                      }),//库存数量 //
                           Formulas.Ext("UNIT_CONVERT", "SECOND_QTY",
                                         new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                      Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())),
                                                      OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                                      OOQL.CreateProperty("ITEM.SECOND_UNIT_ID"),
                                                      OOQL.CreateConstants(0)
                                                      }),//第二数量 //
                           Formulas.Case(null,
                                         OOQL.CreateConstants("1"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CHARACTER"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("2")) |
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.ITEM_TYPE"),OOQL.CreateConstants(string.Empty)).In(OOQL.CreateConstants("2"), OOQL.CreateConstants("3"))) |
                                                                                   (OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty") == OOQL.CreateConstants(0))
                                                                                  ), OOQL.CreateConstants("0"))),
                                         "SETTLEMENT_CLOSE"), //结算状态//
                            Formulas.Case(null,
                                         OOQL.CreateConstants("0"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SYNERGY_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SIGN_TYPE"),OOQL.CreateConstants(string.Empty)) != OOQL.CreateConstants("1")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_TYPE"), OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("2"))
                                                                                  ), OOQL.CreateConstants("1"))),
                                         "INNER_SETTLEMENT_CLOSE"), //内部结算码//
                            Formulas.Case(null,
                                         OOQL.CreateConstants("OTHER"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue()))
                                                                                  ), OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D"))),
                                         "SYNERGY_SOURCE_ID_RTK"), //协同源单类型//
                            Formulas.Case(null,
                                         OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue()))
                                                                                  ), Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())))),
                                         "SYNERGY_SOURCE_ID_ROid"), //协同源单//
                            Formulas.Case(null,
                                         OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue()))
                                                                                  ), Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SYNERGY_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())))),
                                         "SYNERGY_ID"), // 协同关系//
                            Formulas.Case(null,
                                         OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue()))
                                                                                  ), Formulas.IsNull(OOQL.CreateConstants("PURCHASE_ARRIVAL_TEMP.SYNERGY_D_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())))),
                                         "SYNERGY_D_ID"), //协同序号//
                            Formulas.Case(null,
                                         OOQL.CreateConstants("COST_DOMAIN"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants(1))
                                                                                  ), OOQL.CreateConstants("COMPANY"))),
                                         "COST_DOMAIN_ID_RTK"), //成本域//
                            Formulas.Case(null,
                                         OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants(1))), OOQL.CreateProperty("PLANT.COMPANY_ID")),
                                                              OOQL.CreateCaseItem(((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants(2))), OOQL.CreateProperty("PLANT.COST_DOMAIN_ID")),
                                                              OOQL.CreateCaseItem(((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants(3))), OOQL.CreateProperty("WAREHOUSE.COST_DOMAIN_ID"))),
                                         "COST_DOMAIN_ID_ROid"), //成本域类型//
                            Formulas.Case(null,
                                         OOQL.CreateConstants("0"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SYNERGY_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("SALES_SYNERGY.SALES_SYNERGY_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue()))
                                                                                  ), OOQL.CreateConstants("1")),
                                                              OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SYNERGY_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_TYPE"), OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("1"))
                                                                                  ), OOQL.CreateConstants("2")),
                                                              OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SYNERGY_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID"), OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("2"))
                                                                                  ), OOQL.CreateConstants("3")),
                                                              OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D")) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SYNERGY_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue())) != OOQL.CreateConstants(Maths.GuidDefaultValue())) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID"), OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("3"))
                                                                                  ), OOQL.CreateConstants("4"))),
                                         "SYNERGY_TYPE"), //协同关系类型//
                           Formulas.Case(null,
                                         OOQL.CreateConstants("OTHER"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.INTERNAL"),OOQL.CreateConstants(0,GeneralDBType.Boolean)) == OOQL.CreateConstants(1,GeneralDBType.Boolean)) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.REFERENCE_SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("INNER_ORDER_DOC.INNER_ORDER_DOC_D.INNER_ORDER_DOC_SD"))
                                                                                  ), OOQL.CreateConstants("INNER_ORDER_DOC.INNER_ORDER_DOC_D.INNER_ORDER_DOC_SD")),
                                                              OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.INTERNAL"),OOQL.CreateConstants(0,GeneralDBType.Boolean)) == OOQL.CreateConstants(0,GeneralDBType.Boolean)) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD"))
                                                                                  ), OOQL.CreateConstants("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD"))),
                                         "ORDER_SOURCE_ID_RTK"), //订单来源类型//
                            Formulas.Case(null,
                                         OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.INTERNAL"),OOQL.CreateConstants(0,GeneralDBType.Boolean)) == OOQL.CreateConstants(1,GeneralDBType.Boolean)) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.REFERENCE_SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("INNER_ORDER_DOC.INNER_ORDER_DOC_D.INNER_ORDER_DOC_SD"))
                                                                                  ), Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue()))),
                                                              OOQL.CreateCaseItem(((Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.INTERNAL"),OOQL.CreateConstants(0,GeneralDBType.Boolean)) == OOQL.CreateConstants(0,GeneralDBType.Boolean)) &
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_RTK"),OOQL.CreateConstants(string.Empty)) == OOQL.CreateConstants("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD"))
                                                                                  ), Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ID_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue())))),
                                         "ORDER_SOURCE_ID_ROid"), //订单来源//
                           Formulas.Case(null,
                                         OOQL.CreateConstants("1"),
                                         OOQL.CreateCaseArray(OOQL.CreateCaseItem(((OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT") == OOQL.CreateConstants(0,GeneralDBType.Boolean)) |
                                                                                   (Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.PURCHASE_TYPE"),OOQL.CreateConstants(string.Empty)).In(OOQL.CreateConstants("3"), OOQL.CreateConstants("4"))) |
                                                                                   (OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty") == OOQL.CreateConstants(0))
                                                                                  ), OOQL.CreateConstants("0"))),
                                         "SN_COLLECTED_STATUS"), //序列号检核码//
                           OOQL.CreateConstants(0, GeneralDBType.Int32, "ESTI_REVERSE_MODE"),//
                           OOQL.CreateConstants(0, GeneralDBType.Boolean, "SUPPLIER_IN_OUT"),//
                           OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "COST_ELEMENT_ID"),//
                           OOQL.CreateConstants(0, GeneralDBType.Int32, "SETTLEMENT_PATH_TYPE"),//
                           OOQL.CreateConstants(0, GeneralDBType.Int32, "RE_SETTLEMENT_PATH_TYPE"),//
                           OOQL.CreateConstants(1, GeneralDBType.Boolean, "SETTLEMENT_START_INDICATOR"),//
                           OOQL.CreateConstants(OrmDataOption.EmptyDateTime, GeneralDBType.DateTime, "ESTI_PAYMENT_DATE"),//
                           OOQL.CreateConstants(0, GeneralDBType.Boolean, "DEDUCT_ARRIVED_QTY"),//
                           OOQL.CreateConstants(0m, GeneralDBType.Decimal, "SCRAP_BUSINESS_QTY"),//
                           OOQL.CreateConstants(0m, GeneralDBType.Decimal, "RETURN_BUSINESS_QTY"),//
                           //20170619 add by zhangcn for P001-170606002 ===beigin===
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.BUDGET_ADMIN_UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BUDGET_ADMIN_UNIT_ID"),
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.BUDGET_GROUP_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BUDGET_GROUP_ID"),
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.BUDGET_ITEM_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BUDGET_ITEM_ID"),
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.BUDGET_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"PRE_BUDGET_ID"),
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.BUDGET_D_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"PRE_BUDGET_D_ID"),
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ORDER_ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"SOURCE_ORDER_ROid"),
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SOURCE_ORDER_RTK"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"SOURCE_ORDER_RTK"),
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.BUDGET_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BUDGET_ID"),
                           Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.BUDGET_D_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BUDGET_D_ID"),
                           OOQL.CreateConstants(1,"SYNERGY_SETTLEMENT_GROUP"),
                           OOQL.CreateConstants(Maths.GuidDefaultValue(),"TO_ASSET_SOURCE_ID_ROid"),
                           OOQL.CreateConstants("OTHER","TO_ASSET_SOURCE_ID_RTK"),
                           OOQL.CreateConstants(0,"TO_ASSET_STATUS"),
                           //20170619 add by zhangcn for P001-170606002 ===end===
                           Formulas.GetDate("CreateDate")
            });
            #endregion

            QueryNode groupNode = GroupNode(false); //子查询的节点

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
                   .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no")) &
                       (OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")))
                   .LeftJoin("UNIT", "UNIT")
                   .On(OOQL.CreateProperty("UNIT.UNIT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_unit_no"))
                   .LeftJoin("WAREHOUSE", "WAREHOUSE")
                   .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.warehouse_no")) &
                       (OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                   .LeftJoin("WAREHOUSE.BIN", "BIN")
                   .On((OOQL.CreateProperty("BIN.BIN_CODE") ==
                        OOQL.CreateProperty("TEMP_SCAN_DETAIL.storage_spaces_no")) &
                       (OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID")))
                   .LeftJoin(purchaseArrivalTemp, "PURCHASE_ARRIVAL_TEMP")
                   .On((OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.DOC_NO") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.doc_no")) &
                       (OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SequenceNumber") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.seq")))
                   .LeftJoin("ITEM_LOT", "ITEM_LOT")
                   .On((OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.lot_no")) &
                       (OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")) &
                       (((OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no") == OOQL.CreateConstants(string.Empty)) &
                         (OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))) |
                        ((OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no") != OOQL.CreateConstants(string.Empty)) &
                         (OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == 
                         Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()))))))
                   .LeftJoin("SALES_SYNERGY", "SALES_SYNERGY")
                   .On(OOQL.CreateProperty("SALES_SYNERGY.SALES_SYNERGY_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SYNERGY_ID"))
                   .LeftJoin("SUPPLY_SYNERGY", "SUPPLY_SYNERGY")
                   .On(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_TEMP.SYNERGY_ID"));

            QueryNode insertNode = OOQL.Insert("PURCHASE_RECEIPT.PURCHASE_RECEIPT_D", _queryNode, properties.Select(c => c.Alias).ToArray());
            _querySrv.ExecuteNoQuery(insertNode);
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
                OOQL.CreateConstants("PURCHASE_RECEIPT.PURCHASE_RECEIPT_D","SOURCE_ID_RTK"),
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
                , Formulas.IsNull(OOQL.CreateProperty("PURCHASE_RECEIPT.Owner_Org.RTK"),OOQL.CreateConstants(string.Empty), "Owner_Org_RTK")
                , Formulas.IsNull(OOQL.CreateProperty("PURCHASE_RECEIPT.Owner_Org.ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org_ROid")
                , Formulas.IsNull(OOQL.CreateProperty("PURCHASE_RECEIPT.PURCHASE_RECEIPT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_DOC_ID")
                , Formulas.IsNull(OOQL.CreateProperty("PURCHASE_RECEIPT.DOC_DATE"),OOQL.CreateConstants(OrmDataOption.EmptyDateTime), "DOC_DATE")
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
                     .LeftJoin("PURCHASE_RECEIPT", "PURCHASE_RECEIPT")
                     .On(OOQL.CreateProperty("TEMP_SCAN.Table_scan_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT.PURCHASE_RECEIPT_ID"))
                     //20170330 add by wangrm for P001-170328001=====end=======
                    .Where(OOQL.CreateProperty("TEMP_SCAN_DETAIL.barcode_no") != OOQL.CreateConstants(string.Empty));

            QueryNode insertNode = OOQL.Insert("BC_LINE", _queryNode, properties.Select(c => c.Alias).ToArray());
            _querySrv.ExecuteNoQuery(insertNode);
        }

        /// <summary>
        /// 查询采购入库单
        /// </summary>
        /// <param name="category">种类</param>
        /// <param name="reportDatetime">单据日期</param>
        /// <returns></returns>
        private DataTable QueryTempScan(string category, DateTime reportDatetime,object docId){//20170209 modi by liwei1 for P001-170203001 增加参数:docId
            _queryNode =
                OOQL.Select(OOQL.CreateProperty("TEMP_SCAN.Table_scan_ID", "PURCHASE_RECEIPT_ID"),
                            OOQL.CreateProperty("TEMP_SCAN.site_no", "site_no"),//用于其他列计算，插入表之前需要删除该列
                            OOQL.CreateProperty("TEMP_SCAN.info_lot_no", "info_lot_no"),//用于其他列计算，插入表之前需要删除该列
                            OOQL.CreateProperty("DOC.SEQUENCE_DIGIT", "SEQUENCE_DIGIT"),//用于其他列计算，插入表之前需要删除该列
                            //OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID", "DOC_ID"),//20170209 mark by liwei1 for P001-170203001
                            Formulas.Cast(OOQL.CreateConstants(docId), GeneralDBType.Guid, "DOC_ID"),//20170209 add by liwei1 for P001-170203001
                            Formulas.Cast(OOQL.CreateConstants(reportDatetime), GeneralDBType.Date, "DOC_DATE"),
                            Formulas.Cast(OOQL.CreateConstants(""), GeneralDBType.String, 40, "DOC_NO"),//需要计算
                            Formulas.Cast(OOQL.CreateConstants(reportDatetime), GeneralDBType.Date, "TRANSACTION_DATE"),
                            OOQL.CreateConstants(category, "CATEGORY"),
                            OOQL.CreateConstants("PLANT", "Owner_Org_RTK"),
                            Formulas.IsNull(OOQL.CreateProperty("PLANT.PLANT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org_ROid"),
                            Formulas.IsNull(OOQL.CreateProperty("PLANT.COMPANY_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "COMPANY_ID"),
                            Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Emp"),
                            Formulas.IsNull(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Dept"),
                            Formulas.Cast(OOQL.CreateConstants(0m), GeneralDBType.Decimal, "SUM_BUSINESS_QTY"),
                            Formulas.Cast(OOQL.CreateConstants(""), GeneralDBType.String, 40, "VIEW_DOC_NO"),//需要计算
                            OOQL.CreateConstants("", "REMARK"),
                            Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Int32, "PIECES"),
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.SUPPLIER_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SUPPLIER_ID"),
                            Formulas.IsNull(OOQL.CreateProperty("SUPPLIER.SUPPLIER_FULL_NAME"), OOQL.CreateConstants(string.Empty), "SUPPLIER_FULL_NAME"),
                //20170619 add by zhangcn for P001-170606002 ===beigin===
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.ALL_SYNERGY"), OOQL.CreateConstants(false), "ALL_SYNERGY"),
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.GROUP_SYNERGY_ID.ROid"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "GROUP_SYNERGY_ID_ROid"),
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.GROUP_SYNERGY_ID.RTK"), OOQL.CreateConstants(string.Empty), "GROUP_SYNERGY_ID_RTK"),
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.SOURCE_SUPPLIER_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_SUPPLIER_ID"),
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_Sequence"), OOQL.CreateConstants(0, GeneralDBType.Int32), "DOC_Sequence"),
                            OOQL.CreateConstants(string.Empty, "GENERATE_NO"),
                            OOQL.CreateConstants(false, "GENERATE_STATUS"),
                            Formulas.Cast(OOQL.CreateConstants(Maths.GuidDefaultValue()), GeneralDBType.Guid, "GROUP_SYNERGY_D_ID"), //需要计算
                //20170619 add by zhangcn for P001-170606002 ===end===
                            Formulas.Cast(OOQL.CreateConstants(Maths.GuidDefaultValue()), GeneralDBType.Guid, "WAREHOUSE_ID"),
                            Formulas.Cast(OOQL.CreateConstants("N"), GeneralDBType.String, 2, "ApproveStatus"),
                            Formulas.GetDate("CreateDate"))
                    .From(_TEMP_SCAN.Name, "TEMP_SCAN")
                    .LeftJoin("EMPLOYEE", "EMPLOYEE")
                    .On(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE") == OOQL.CreateProperty("TEMP_SCAN.employee_no"))
                    .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                    .On(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE") == OOQL.CreateProperty("TEMP_SCAN.picking_department_no"))
                    .LeftJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateProperty("TEMP_SCAN.site_no"))
                    //20170209 mark by liwei1 for P001-170123001 ===begin===
                    //.LeftJoin("PARA_DOC_FIL", "PARA_DOC_FIL")
                    //.On((OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")) &
                    //    (OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants(category)))
                    //20170209 mark by liwei1 for P001-170123001 ===end===
                    .LeftJoin("DOC", "DOC")
                    .On(OOQL.CreateProperty("DOC.DOC_ID") == OOQL.CreateConstants(docId))//20170209 mark by liwei1 for P001-170123001 odl:OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID"))
                    .LeftJoin("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                    .On(OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO") == OOQL.CreateProperty("TEMP_SCAN.info_lot_no"))
                    .LeftJoin("SUPPLIER", "SUPPLIER")
                    .On(OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.SUPPLIER_ID"));

            return _querySrv.Execute(_queryNode);
        }

        /// <summary>
        /// 到货单查询语句QueryNode
        /// </summary>
        /// <returns></returns>
        private QueryNode QueryPurchaseArrival() {
            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("PURCHASE_ARRIVAL.SYNERGY_ID", "SYNERGY_ID"),//到货单.协同关系
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.SYNERGY_D_ID", "SYNERGY_D_ID"),//到货单.协同序号
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO", "DOC_NO"),//到货单.单号
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SequenceNumber", "SequenceNumber"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID", "PURCHASE_ARRIVAL_D_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_TYPE", "ITEM_TYPE"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_TYPE", "PURCHASE_TYPE"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.REFERENCE_SOURCE_ID.RTK", "REFERENCE_SOURCE_ID_RTK"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.REFERENCE_SOURCE_ID.ROid", "REFERENCE_SOURCE_ID_ROid"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SOURCE_ID.RTK", "SOURCE_ID_RTK"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SOURCE_ID.ROid", "SOURCE_ID_ROid"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PRICE_UNIT_ID", "PRICE_UNIT_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PACKING_MODE_ID", "PACKING_MODE_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SIGN_TYPE", "SIGN_TYPE"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_DESCRIPTION", "ITEM_DESCRIPTION"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_SPECIFICATION", "ITEM_SPECIFICATION"),
                            Formulas.Cast(OOQL.CreateConstants("N"), GeneralDBType.String, 2, "ApproveStatus"),
                            OOQL.CreateProperty("DOC.INTERNAL", "INTERNAL"),//单据性质.内部交易
                //20170619 add by zhangcn for P001-170606002 ===beigin===
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUDGET_ADMIN_UNIT_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUDGET_GROUP_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUDGET_ITEM_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUDGET_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUDGET_D_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SOURCE_ORDER.ROid", "SOURCE_ORDER_ROid"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SOURCE_ORDER.RTK", "SOURCE_ORDER_RTK")
                //20170619 add by zhangcn for P001-170606002 ===end===
                            )
                    .From("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                    .InnerJoin("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                    .On(OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID") ==
                        OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID"))
                    .InnerJoin("DOC", "DOC")
                    .On(OOQL.CreateProperty("DOC.DOC_ID") ==
                        OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_ID"));
            return queryNode;
        }

        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="isEntityLine"></param>
        /// <returns></returns>
        public QueryNode GroupNode(bool isEntityLine) {
            List<QueryProperty> properties = new List<QueryProperty>{
                    OOQL.CreateProperty("TMP.Table_scan_detail_ID")//
                    , OOQL.CreateProperty("TMP.info_lot_no")//
                    , OOQL.CreateProperty("TMP.site_no")
                    , OOQL.CreateProperty("TMP.SequenceNumber")
                    , OOQL.CreateProperty("TMP.item_no")//
                    , OOQL.CreateProperty("TMP.item_feature_no")//
                    , OOQL.CreateProperty("TMP.picking_unit_no")//
                    , OOQL.CreateProperty("TMP.doc_no")//
                    , OOQL.CreateProperty("TMP.seq")//
                    , OOQL.CreateProperty("TMP.warehouse_no")//
                    , OOQL.CreateProperty("TMP.storage_spaces_no")//
                    , OOQL.CreateProperty("TMP.lot_no") //
            };

            List<QueryProperty> groupProperties = new List<QueryProperty>();
            groupProperties = new List<QueryProperty>();
            groupProperties.AddRange(properties);

            if (isEntityLine) {
                properties.Add(OOQL.CreateProperty("TMP.barcode_no"));
                groupProperties.Add(OOQL.CreateProperty("TMP.barcode_no"));
            }

            properties.Add(Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("TMP.picking_qty")), OOQL.CreateConstants(0), "picking_qty"));

            QueryNode node = OOQL.Select(properties)
                                 .From(_TEMP_SCAN_DETAIL.Name, "TMP")
                                 .GroupBy(groupProperties);

            return node;
        }

        /// <summary>
        /// 查询采购入库单，用于自动签核
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection QueryPurchase() {
            _queryNode =
                OOQL.Select(OOQL.CreateProperty("PURCHASE_RECEIPT.DOC_ID", "DOC_ID"),
                            OOQL.CreateProperty("PURCHASE_RECEIPT.Owner_Org.ROid", "Owner_Org_ROid"),
                            OOQL.CreateProperty("PURCHASE_RECEIPT.PURCHASE_RECEIPT_ID", "PURCHASE_RECEIPT_ID"))
                    .From(_TEMP_SCAN.Name, "TEMP_SCAN")
                    .InnerJoin("PURCHASE_RECEIPT", "PURCHASE_RECEIPT")
                    .On(OOQL.CreateProperty("PURCHASE_RECEIPT.PURCHASE_RECEIPT_ID") ==
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

        #endregion

        #region 辅助方法
        //20170209 add by liwei1 for P001-170203001 ===begin===
        /// <summary>
        /// 查询单据性质ID
        /// </summary>
        /// <param name="category">单据性质</param>
        /// <param name="scanColl">单身数据集</param>
        /// <returns>单据性质ID</returns>
        private object QueryDocId(string category, DependencyObjectCollection scanColl) {
            QueryNode node = null;
            if (scanColl.Count > 0) {
                string infoLotNo = scanColl[0]["info_lot_no"].ToStringExtension(); //信息批号
                string siteNo = scanColl[0]["site_no"].ToStringExtension(); //工厂
                //根据条件查询满足条件的DOC_ID
                node = OOQL.Select(1, OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID"))
                    .From("PARA_DOC_FIL", "PARA_DOC_FIL")
                    .InnerJoin("PLANT", "PLANT")
                    .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid")))
                    .Where((OOQL.AuthFilter("PARA_DOC_FIL", "PARA_DOC_FIL"))
                           & ((OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo))
                           & (OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants(category))
                           & ((OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                            | (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.Select(1, OOQL.CreateProperty("DOC_ID"))
                                .From("PURCHASE_ARRIVAL")
                                .Where((OOQL.CreateProperty("DOC_NO") == OOQL.CreateConstants(infoLotNo)))))))
                    .OrderBy(
                        OOQL.CreateOrderByItem(
                            OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"), SortType.Desc));
            }
            return GetService<IQueryService>().ExecuteScalar(node);
        }
        //20170209 add by liwei1 for P001-170203001 ===end===

        /// <summary>
        ///     通过DataTable列名转换为ColumnMappings
        /// </summary>
        /// <param name="columns">表的列的集合</param>
        /// <returns>Mapping集合</returns>
        private Collection<BulkCopyColumnMapping> GetBulkCopyColumnMapping(DataColumnCollection columns) {
            var mapping = new Collection<BulkCopyColumnMapping>();
            foreach (DataColumn column in columns) {
                //列名
                var targetName = column.ColumnName;
                //列名中的下划线大于0，且以[_RTK]或[_ROid]结尾的列名视为多来源字段
                if ((targetName.IndexOf("_") > 0)
                    && (targetName.EndsWith("_RTK", StringComparison.CurrentCultureIgnoreCase)
                        || targetName.EndsWith("_ROid", StringComparison.CurrentCultureIgnoreCase))) {
                    //列名长度
                    var nameLength = targetName.Length;
                    //最后一个下划线后一位位置
                    var endPos = targetName.LastIndexOf("_") + 1;
                    //拼接目标字段名
                    targetName = targetName.Substring(0, endPos - 1) + "." +
                                 targetName.Substring(endPos, nameLength - endPos);
                }
                var mappingItem = new BulkCopyColumnMapping(column.ColumnName, targetName);
                //mapping结合中不存在便添加元数
                if (!mapping.Contains(mappingItem))
                    mapping.Add(mappingItem);
            }
            return mapping;
        }

        /// <summary>
        ///     产生不重复的单据号
        /// </summary>
        /// <param name="docNo">原单号</param>
        /// <returns>新单号</returns>
        private string CreateNewDocNo(string docNo) {
            var number = docNo.Remove(0, docNo.Length - 4).ToInt32() + 1; //获取单号的 流水号 并加1
            var newNumber = number.ToString().PadLeft(4, '0'); //新流水号补位
            var startNo = docNo.Substring(0, docNo.Length - 4); //原单号起始位

            return startNo + newNumber;
        }

        /// <summary>
        ///     按照特定格式拼接Message提示信息
        /// </summary>
        /// <param name="docNoList">拼接之后集合</param>
        /// <param name="docNo">需要拼接单号</param>
        /// <returns>返回新拼接后的集合</returns>
        private string MessageFormat(List<string> docNoList, string docNo) {
            if (docNoList.Count % 5 == 0)
                docNo = "\n" + docNo;
            return docNo;
        }


        /// <summary>
        /// 分组之后的实体行信息
        /// </summary>
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

        //20170619 add by zhangcn for P001-170606002 ===beigin===
        private DependencyObjectCollection QuerySupplySyneryFiD(object syneryId) {
            QueryNode queryNode =
                OOQL.Select(1,
                            OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SUPPLY_SYNERGY_FI_D_ID", "SUPPLY_SYNERGY_FI_D_ID"),
                            OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SequenceNumber", "SequenceNumber"))
                    .From("SUPPLY_SYNERGY.SUPPLY_SYNERGY_FI_D", "SUPPLY_SYNERGY_FI_D")
                    .Where(OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SUPPLY_SYNERGY_ID") == OOQL.CreateConstants(syneryId))
                    .OrderBy(new OrderByItem(OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SequenceNumber"), SortType.Asc));

            return _querySrv.ExecuteDependencyObject(queryNode);
        }

        //20170619 add by zhangcn for P001-170606002 ===end===
    }
}

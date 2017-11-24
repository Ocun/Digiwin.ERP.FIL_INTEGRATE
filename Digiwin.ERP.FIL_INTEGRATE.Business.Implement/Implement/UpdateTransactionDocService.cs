//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-14</createDate>
//<description>生成采购入库单 实现</description>
//----------------------------------------------------------------
//20170713 modi by zhangcn for B002-170713010 解决[更新所有出入库单的单头业务数量合计字段]问题
//20170719 modi by zhangcn for B001-170717002 解决【单头数量合计字段，赋值不正确】问题
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
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    ///更新出入库单服务
    /// </summary>
    [ServiceClass(typeof(IUpdateTransactionDocService))]
    [Description("更新出入库单服务")]
    public class UpdateTransactionDocService : ServiceComponent, IUpdateTransactionDocService {
        #region 属性

        private IQueryService _querySrv;
        private IBusinessTypeService _businessTypeSrv;
        private QueryNode _queryNode;
        private IDataEntityType _TEMP_SCAN_DETAIL;

        private DependencyObjectCollection _collDocNos;
        private IInfoEncodeContainer _encodeSrv;
        private ISysParameterService _sysParameterSrv;
        List<QueryProperty> _lstQueryProperties;
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
            get { return _primaryKeySrv ?? (_primaryKeySrv = GetService<IPrimaryKeyService>("TRANSFER_DOC")); }
        }

        #endregion

        #region 接口方法
        /// <summary>
        /// 更新库存交易单单身
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
        public DependencyObjectCollection DoUpdateTransactionDoc(string employeeNo, string scanType, DateTime reportDatetime,
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
            _collDocNos = CreateReturnCollection();
            List<string> lstDocNos = new List<string>();
            _lstQueryProperties = new List<QueryProperty>();
            //DataTable dtTempScan = null;//临时表Scan的DataTable结构
            DataTable dtTempScanDetail = null;//临时表ScanDetail的DataTable结构
            List<BulkCopyColumnMapping> lstColumnMappingsScanDetail = null;//临时表ScanDetail的映射列
            CreateTempTableMapping(ref dtTempScanDetail, ref lstColumnMappingsScanDetail);//创建临时表#Table_scan的列映射
            InsertTempTableData(dtTempScanDetail, collScan, lstDocNos);//插入DataTable数据
            #endregion

            using (var transService = GetService<ITransactionService>()) {
                using (var myConnectionSr = GetService<IConnectionService>()) {

                    InitParameters();//初始化全局变量

                    #region 7.1更新出入库单实体

                    //创建临时表Scan与ScanDetail
                    CreateTempTableScanDetail();
                    _querySrv.BulkCopy(dtTempScanDetail, _TEMP_SCAN_DETAIL.Name, lstColumnMappingsScanDetail.ToArray());//批量插入数据到临时表ScanDetail

                    int decimalPlace = 8;//金额小数位数
                    int carryMode = 1;//金额取位模式
                    object companyId = Maths.GuidDefaultValue();
                    DependencyObjectCollection collPalnt = QueryPlant();
                    if (collPalnt.Count > 0) {
                        companyId = collPalnt[0]["COMPANY_ID"];

                        //调用系统参数服务 获取 记账本位币
                        object currencyId = _sysParameterSrv.GetValue("FUNCTION_CURRENCY_ID", companyId);

                        //Call 货币单价金额取位服务
                        ICurrencyPrecisionService currencyPrecisionSrv = GetServiceForThisTypeKey<ICurrencyPrecisionService>();
                        object[] arrPrecision = currencyPrecisionSrv.GetPrecision(companyId, currencyId);
                        decimalPlace = arrPrecision[0].ToInt32();
                        carryMode = arrPrecision[1].ToInt32();
                    }

                    UpdateTransactionDocD(decimalPlace, carryMode);//更新库存交易单单身
                    UpdateTransactionDoc();//更新库存交易单单头
                    DependencyObjectCollection collParaFil = QueryParaFil();
                    if (collParaFil.Count > 0 && collParaFil[0]["BC_LINE_MANAGEMENT"].ToBoolean()){
                        DeleteBcLine();//删除条码交易明细 BC_LINE
                        InsertBcLine();//插入条码交易明细 BC_LINE
                    }

                    #endregion

                    //7.2审核
                   
                    //保存单据
                    DependencyObjectCollection collTransactionDoc = QueryDoc(lstDocNos);
                    IReadService readSrv = GetService<IReadService>("TRANSACTION_DOC");
                    object[] entities = readSrv.Read(collTransactionDoc.Select(c => c["TRANSACTION_DOC_ID"]).ToArray());
                    if (entities != null && entities.Length > 0) {
                        ISaveService saveSrv = GetService<ISaveService>("TRANSACTION_DOC");
                        saveSrv.Save(entities);
                    }

                    //保存时没有自动审核的，需要重新审核

                    entities = readSrv.Read(collTransactionDoc.Where(c => c["AUTO_APPROVE"].ToBoolean() == false).Select(c => c["TRANSACTION_DOC_ID"]).ToArray());
                    IConfirmService confirmService = GetService<IConfirmService>("TRANSACTION_DOC");
                    ILogOnService logOnSrv = GetService<ILogOnService>();
                    foreach (DependencyObject obj in entities) {
                        ConfirmContext context = new ConfirmContext(obj.Oid, logOnSrv.CurrentUserId, reportDatetime.ToDate());
                        confirmService.Execute(context);
                    }
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
            _sysParameterSrv = GetService<ISysParameterService>();
            
        }

        #region 临时表

        /// <summary>
        /// 创建临时表#Table_scan_detail
        /// </summary>
        /// <returns></returns>
        private void CreateTempTableScanDetail() {
            string tempName = "TEMP_SCAN_DETAIL" + "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { });
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);

            #region 字段
            ////主键
            //defaultType.RegisterSimpleProperty("Table_scan_detail_ID", _businessTypeSrv.SimplePrimaryKeyType,
            //                                   Maths.GuidDefaultValue(), false,
            //                                   new Attribute[] { _businessTypeSrv.SimplePrimaryKey });
            //信息批号
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string),
                                              string.Empty, false, new Attribute[] { tempAttr });

            // 转入仓库
            defaultType.RegisterSimpleProperty("site_no", typeof(string),
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

          
            #endregion

            _TEMP_SCAN_DETAIL = defaultType;

            _querySrv.CreateTempTable(defaultType);
        }

        /// <summary>
        /// 创建临时表#Table_scan的列映射
        /// </summary>
        /// <param name="dtTempScanDetail"></param>
        /// <param name="lstColumnMappingsScanDetail"></param>
        private void CreateTempTableMapping(ref DataTable dtTempScanDetail, ref List<BulkCopyColumnMapping> lstColumnMappingsScanDetail) {
            #region ScanDetail表
            string[] scanColumnsDetail = new string[]{
                   // "Table_scan_detail_ID",  //主键
                    "info_lot_no",  //信息批号
                    "site_no", //转入库位     
                    "barcode_no",  
                    "item_no",
                    "item_feature_no",                   
                    "warehouse_no",                   
                    "storage_spaces_no",                   
                    "lot_no",                    
                    "picking_qty",                    
                    "picking_unit_no",                   
                    "doc_no",                    
                    "seq"//工厂
                             
            };
            dtTempScanDetail = UtilsClass.CreateDataTable("", scanColumnsDetail,
                    new Type[]{
                       // typeof(object),    //主键
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
        /// <param name="dtTempScanDetail"></param>
        /// <param name="collScan"></param>
        /// <param name="lstDocNos"></param>
        private void InsertTempTableData(DataTable dtTempScanDetail, DependencyObjectCollection collScan, List<string> lstDocNos) {
            foreach (DependencyObject objScan in collScan) {
                _lstQueryProperties.Add(OOQL.CreateConstants(objScan["site_no"]));//供后面查询使用

                #region ScanDetail 单头与单身中的info_lot_no有关联

                DependencyObjectCollection collScanDetail = objScan["scan_detail"] as DependencyObjectCollection;
                if (collScanDetail != null && collScanDetail.Count > 0) {
                    foreach (DependencyObject objScanDetail in collScanDetail) {
                        DataRow drScanDetail = dtTempScanDetail.NewRow();
                        // drScanDetail["Table_scan_detail_ID"] = _primaryKeySrv.CreateId("TRANSFER_DOC.TRANSFER_DOC_D");
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

                        //记录单号，后续保存和审核需要使用
                        string docNo = objScanDetail["doc_no"].ToStringExtension();
                        if (!Maths.IsEmpty(docNo) && !lstDocNos.Contains(docNo)) {
                            lstDocNos.Add(docNo);

                            //相同的单号只返回一次
                            DependencyObject objDocNo = _collDocNos.AddNew();
                            objDocNo["doc_no"] = docNo;  //用于主方法返回值
                        }
                    }
                }
                #endregion
            }
        }

        #endregion

        #region 更新操作

        /// <summary>
        /// 更新调拨单单身
        /// </summary>
        /// <param name="decimalPlace">金额小数位数</param>
        /// <param name="carryMode">金额取位模式</param>
        private void UpdateTransactionDocD(int decimalPlace, int carryMode) {
            #region 查询

            QueryNode groupNode = GroupNode(false); //子查询的节点

            _queryNode =
                OOQL.Select(OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_D_ID", "TRANSACTION_DOC_D_ID"),
                    OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty", "BUSINESS_QTY"),
                    Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()),
                        "UNIT_ID"),
                    Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),
                        OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),
                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()),
                        "BIN_ID"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"),
                        OOQL.CreateConstants(Maths.GuidDefaultValue()), "ITEM_LOT_ID"),
                    Formulas.Ext("UNIT_CONVERT", "SECOND_QTY",
                        new object[]{
                            OOQL.CreateProperty("ITEM.ITEM_ID"),
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),
                                OOQL.CreateConstants(Maths.GuidDefaultValue())),
                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                            OOQL.CreateProperty("ITEM.SECOND_UNIT_ID"),
                            OOQL.CreateConstants(0)
                        }), //领料第二数量
                    Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY",
                        new object[]{
                            OOQL.CreateProperty("ITEM.ITEM_ID"),
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),
                                OOQL.CreateConstants(Maths.GuidDefaultValue())),
                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                            OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                            OOQL.CreateConstants(0)
                        }), //领料库存数量
                    Formulas.Case(null,
                        OOQL.CreateConstants("COST_DOMAIN"),
                        OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                ((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") ==
                                  OOQL.CreateConstants(1))
                                    ), OOQL.CreateConstants("COMPANY"))),
                        "COST_DOMAIN_ID_RTK"), //成本域
                    Formulas.Case(null,
                        OOQL.CreateConstants(Maths.GuidDefaultValue()),
                        OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                ((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") ==
                                  OOQL.CreateConstants(1))), OOQL.CreateProperty("PLANT.COMPANY_ID")),
                            OOQL.CreateCaseItem(
                                ((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") ==
                                  OOQL.CreateConstants(2))), OOQL.CreateProperty("PLANT.COST_DOMAIN_ID")),
                            OOQL.CreateCaseItem(
                                ((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") ==
                                  OOQL.CreateConstants(3))), OOQL.CreateProperty("WAREHOUSE.COST_DOMAIN_ID"))),
                        "COST_DOMAIN_ID_ROid"), //成本域类型
                    OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT", "ITEM_SN_MANAGEMENT"),
                    Formulas.Case(null,
                        OOQL.CreateConstants("1"),
                        OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                ((OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT") ==
                                  OOQL.CreateConstants(0, GeneralDBType.Boolean)) |
                                 (Formulas.Ext("UNIT_CONVERT",
                                     new object[]{
                                         OOQL.CreateProperty("ITEM.ITEM_ID"),
                                         Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),
                                             OOQL.CreateConstants(Maths.GuidDefaultValue())),
                                         OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                         OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                         OOQL.CreateConstants(0)
                                     }) == OOQL.CreateConstants(0))
                                    ), OOQL.CreateConstants("0")),
                            OOQL.CreateCaseItem(
                                ((OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT") ==
                                  OOQL.CreateConstants(1, GeneralDBType.Boolean)) &
                                 (OOQL.CreateProperty("TRANSACTION_DOC_D.SN_COLLECTED_QTY") !=
                                  OOQL.CreateConstants(0, GeneralDBType.Decimal)) &
                                 (Formulas.Abs(Formulas.Ext("UNIT_CONVERT",
                                     new object[]{
                                         OOQL.CreateProperty("ITEM.ITEM_ID"),
                                         Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),
                                             OOQL.CreateConstants(Maths.GuidDefaultValue())),
                                         OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                         OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                         OOQL.CreateConstants(0)
                                     })) == OOQL.CreateProperty("TRANSACTION_DOC_D.SN_COLLECTED_QTY"))
                                    ), OOQL.CreateConstants("2"))),
                        "SN_COLLECTED_STATUS"), //序列号检核码
                    Formulas.Case(null,
                        Formulas.Round(OOQL.CreateProperty("TRANSACTION_DOC_D.UNIT_COST")*Formulas.Ext("UNIT_CONVERT",
                            new object[]{
                                OOQL.CreateProperty("ITEM.ITEM_ID"),
                                Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),
                                    OOQL.CreateConstants(Maths.GuidDefaultValue())),
                                OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                OOQL.CreateConstants(0)
                            }),
                            decimalPlace, 1, ""),
                        OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(((OOQL.CreateConstants(carryMode) == OOQL.CreateConstants(1))
                                ),
                                Formulas.Round(
                                    OOQL.CreateProperty("TRANSACTION_DOC_D.UNIT_COST")*Formulas.Ext("UNIT_CONVERT",
                                        new object[]{
                                            OOQL.CreateProperty("ITEM.ITEM_ID"),
                                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                OOQL.CreateConstants(Maths.GuidDefaultValue())),
                                            OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                            OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                            OOQL.CreateConstants(0)
                                        }),
                                    decimalPlace))),
                        "COST_AMT") //成本金额
                    )
                    //.From(_TEMP_SCAN_DETAIL.Name, "TEMP_SCAN_DETAIL")
                    .From(groupNode, "TEMP_SCAN_DETAIL")
                    .InnerJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
                    .On(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.doc_no"))
                    .InnerJoin("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                    .On((OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID") ==
                         OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID")) &
                        (OOQL.CreateProperty("TRANSACTION_DOC_D.SequenceNumber") ==
                         OOQL.CreateProperty("TEMP_SCAN_DETAIL.seq")))
                    .InnerJoin("ITEM", "ITEM")
                    .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID")) &
                        (OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_no")))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.site_no"))
                    .InnerJoin("PARA_COMPANY", "PARA_COMPANY")
                    .On(OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.COMPANY_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") ==
                         OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_FEATURE_ID")) &
                        (Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                            OOQL.CreateConstants(string.Empty)) ==
                         OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no")))
                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                    .On((OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.lot_no")) &
                        (OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")) &
                        (OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") ==
                         Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                             OOQL.CreateConstants(Maths.GuidDefaultValue()))))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_unit_no"))
                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") ==
                         OOQL.CreateProperty("TEMP_SCAN_DETAIL.warehouse_no")) &
                        (OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On((OOQL.CreateProperty("BIN.BIN_CODE") ==
                         OOQL.CreateProperty("TEMP_SCAN_DETAIL.storage_spaces_no")) &
                        (OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID")));
                  
            #endregion

            #region 执行修改
            QueryNode updateNode = OOQL.Update("TRANSACTION_DOC.TRANSACTION_DOC_D")
                .Set(new SetItem[]{
                    new SetItem(OOQL.CreateProperty("BUSINESS_QTY"),OOQL.CreateProperty("SelectNode.BUSINESS_QTY")),
                    new SetItem(OOQL.CreateProperty("BUSINESS_UNIT_ID"),OOQL.CreateProperty("SelectNode.UNIT_ID")),
                    new SetItem(OOQL.CreateProperty("SECOND_QTY"),OOQL.CreateProperty("SelectNode.SECOND_QTY")),
                    new SetItem(OOQL.CreateProperty("INVENTORY_QTY"),OOQL.CreateProperty("SelectNode.INVENTORY_QTY")),
                    new SetItem(OOQL.CreateProperty("WAREHOUSE_ID"),OOQL.CreateProperty("SelectNode.WAREHOUSE_ID")),
                    new SetItem(OOQL.CreateProperty("BIN_ID"),OOQL.CreateProperty("SelectNode.BIN_ID")),
                    new SetItem(OOQL.CreateProperty("ITEM_LOT_ID"),OOQL.CreateProperty("SelectNode.ITEM_LOT_ID")),
                    new SetItem(OOQL.CreateProperty("COST_DOMAIN_ID.RTK"),OOQL.CreateProperty("SelectNode.COST_DOMAIN_ID_RTK")),
                    new SetItem(OOQL.CreateProperty("COST_DOMAIN_ID.ROid"),OOQL.CreateProperty("SelectNode.COST_DOMAIN_ID_ROid")),
                    new SetItem(OOQL.CreateProperty("SN_COLLECTED_STATUS"),OOQL.CreateProperty("SelectNode.SN_COLLECTED_STATUS")),
                    new SetItem(OOQL.CreateProperty("COST_AMT"),OOQL.CreateProperty("SelectNode.COST_AMT"))
                })
                .From(_queryNode, "SelectNode")
                .Where(OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_D.TRANSACTION_DOC_D_ID") == OOQL.CreateProperty("SelectNode.TRANSACTION_DOC_D_ID"));
            #endregion

            _querySrv.ExecuteNoQueryWithManageProperties(updateNode);
        }

        /// <summary>
        ///  更新调拨单单头
        /// </summary>
        private void UpdateTransactionDoc(){
            #region 查询
            #region 20170719 modi by zhangcn for  B001-170717002
            //_queryNode =
            //    OOQL.Select(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO", "DOC_NO"),
            //                Formulas.Sum(OOQL.CreateProperty("TRANSACTION_DOC_D.BUSINESS_QTY"), "SUM_BUSINESS_QTY"))
            //        .From(_TEMP_SCAN_DETAIL.Name, "TEMP_SCAN_DETAIL")
            //        .InnerJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
            //        .On(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.doc_no"))
            //        .InnerJoin("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
            //        .On((OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID"))
            //            &(OOQL.CreateProperty("TRANSACTION_DOC_D.SequenceNumber") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.seq")))
            //        .GroupBy(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO")) ;

            _queryNode =
                OOQL.Select(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO", "DOC_NO"),
                            Formulas.Sum(OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"), "SUM_BUSINESS_QTY"))
                    .From(_TEMP_SCAN_DETAIL.Name, "TEMP_SCAN_DETAIL")
                    .InnerJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
                    .On(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.doc_no"))
                    .GroupBy(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO"));
            #endregion
            #endregion

            #region 执行修改

            QueryNode updateNode = OOQL.Update("TRANSACTION_DOC")
                .Set(new SetItem[]{
                    new SetItem(OOQL.CreateProperty("SUM_BUSINESS_QTY"),
                        OOQL.CreateProperty("SelectNode.SUM_BUSINESS_QTY"))
                })
                .From(_queryNode, "SelectNode")
                .Where(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO") == OOQL.CreateProperty("SelectNode.DOC_NO"));//20170713 modi by zhangcn for B002-170713010 还原 Where条件,以前被注释掉了
            #endregion

            _querySrv.ExecuteNoQueryWithManageProperties(updateNode);
        }


        /// <summary>
        /// 删除条码交易明细
        /// </summary>
        private void DeleteBcLine(){
            QueryNode deleteNode = 
                OOQL.Delete("BC_LINE")
                    .Where((OOQL.CreateProperty("SOURCE_ID.RTK") == OOQL.CreateConstants("TRANSACTION_DOC.TRANSACTION_DOC_D")) &
                            OOQL.CreateProperty("SOURCE_ID.ROid")
                                .In(OOQL.Select(OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_D_ID"))
                                        .From("TRANSACTION_DOC.TRANSACTION_DOC_D","TRANSACTION_DOC_D")
                                        .InnerJoin("TRANSACTION_DOC","TRANSACTION_DOC")
                                        .On(OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID"))
                                        .InnerJoin(_TEMP_SCAN_DETAIL.Name, "TEMP_SCAN_DETAIL")
                                        .On((OOQL.CreateProperty("TEMP_SCAN_DETAIL.doc_no") == OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO"))&
                                            (OOQL.CreateProperty("TEMP_SCAN_DETAIL.seq") == OOQL.CreateProperty("TRANSACTION_DOC_D.SequenceNumber")))
                                        ));
           
            _querySrv.ExecuteNoQuery(deleteNode); 
        }

        /// <summary>
        /// 插入条码交易明细
        /// </summary>
        private void InsertBcLine() {
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                Formulas.NewId("BC_LINE_ID"),
                OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_D_ID", "SOURCE_ID_ROid"),
                OOQL.CreateConstants("TRANSACTION_DOC.TRANSACTION_DOC_D","SOURCE_ID_RTK"),
                OOQL.CreateProperty("TEMP_SCAN_DETAIL.barcode_no", "BARCODE_NO"),
                Formulas.Ext("UNIT_CONVERT", "QTY",
                                         new object[]{OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID"),
                                                      Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())),
                                                      OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_qty"),
                                                      OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                      OOQL.CreateConstants(0)
                                                      }),//库存数量
                Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),
                Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID")
            });

            #region 查询
            QueryNode groupNode = GroupNode(true); //子查询的节点
            _queryNode =
                OOQL.Select(properties)
                    .From(groupNode, "TEMP_SCAN_DETAIL")
                    .InnerJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
                    .On(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.doc_no"))
                    .InnerJoin("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                    .On((OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID")) &
                        (OOQL.CreateProperty("TRANSACTION_DOC_D.SequenceNumber") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.seq")))
                    .InnerJoin("ITEM", "ITEM")
                    .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID")) &
                        (OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_no")))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_FEATURE_ID")) &
                        (Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty)) == OOQL.CreateProperty("TEMP_SCAN_DETAIL.item_feature_no")))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.site_no"))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.picking_unit_no"))
                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.warehouse_no")) &
                       (OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On((OOQL.CreateProperty("BIN.BIN_CODE") == OOQL.CreateProperty("TEMP_SCAN_DETAIL.storage_spaces_no")) &
                        (OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID")))
                    .Where(OOQL.CreateProperty("TEMP_SCAN_DETAIL.barcode_no") != OOQL.CreateConstants(string.Empty));

            #endregion

            QueryNode insertNode = OOQL.Insert("BC_LINE", _queryNode, properties.Select(c => c.Alias).ToArray());
            _querySrv.ExecuteNoQuery(insertNode);
        }

        /// <summary>
        /// 查询单号
        /// </summary>
        /// <param name="docNos">单号集合</param>
        /// <returns></returns>
        private DependencyObjectCollection QueryDoc(List<string> docNos) {
            _queryNode = OOQL.Select(OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID"),
                                    OOQL.CreateProperty("DOC.AUTO_APPROVE"))
                .From("TRANSACTION_DOC", "TRANSACTION_DOC")
                .InnerJoin("DOC", "DOC")
                .On(OOQL.CreateProperty("DOC.DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC.DOC_ID"))
                .Where(OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO").In(OOQL.CreateDyncParameter("docnos", docNos.ToArray())));
            return _querySrv.ExecuteDependencyObject(_queryNode);
        }

        /// <summary>
        /// 查询工厂
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection QueryPlant() {
            _queryNode =
               OOQL.Select(1, OOQL.CreateProperty("PLANT.COMPANY_ID", "COMPANY_ID"))
                    .From("PLANT", "PLANT")
                    .Where((OOQL.AuthFilter("PLANT", "PLANT")) &
                            (OOQL.CreateProperty("PLANT.PLANT_CODE").In(_lstQueryProperties.ToArray())));
            return _querySrv.ExecuteDependencyObject(_queryNode);

        }

        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="isEntityLine"></param>
        /// <returns></returns>
        public QueryNode GroupNode(bool isEntityLine) {
            List<QueryProperty> properties = new List<QueryProperty>{
                   // OOQL.CreateProperty("TMP.info_lot_no")
                    OOQL.CreateProperty("TMP.item_no")
                    , OOQL.CreateProperty("TMP.item_feature_no")
                    , OOQL.CreateProperty("TMP.picking_unit_no")
                    , OOQL.CreateProperty("TMP.doc_no")
                    , OOQL.CreateProperty("TMP.seq")
                    , OOQL.CreateProperty("TMP.warehouse_no")
                    , OOQL.CreateProperty("TMP.storage_spaces_no")
                    , OOQL.CreateProperty("TMP.lot_no")
                    , OOQL.CreateProperty("TMP.site_no")    
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

        #endregion
    }
}

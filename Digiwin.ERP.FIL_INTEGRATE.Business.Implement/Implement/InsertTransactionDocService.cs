//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/3/27 15:49:22</CreateDate>
//<IssueNO>P001-170327001</IssueNO>
//<Description>生成出入库单实现</Description>
//---------------------------------------------------------------- 
//20170330 modi by wangrm for P001-170328001
//20170413 modi by wangyq for P001-170412001 新增几个值为出入库单单头的字段

using System;
using System.Collections.Generic;
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
    /// 生成出入库单实现 
    /// </summary>
    [ServiceClass(typeof(IInsertTransactionDocService))]
    [Description("生成出入库单实现")]
    sealed class InsertTransactionDocService : ServiceComponent, IInsertTransactionDocService {
        IQueryService _qurService;
        IDataEntityType _tempScan;
        IDataEntityType _tempScanDetail;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="employee_no">扫描人员</param>
        /// <param name="scan_type">扫描类型1.有箱条码 2.无箱条码</param>
        /// <param name="report_datetime">上传时间</param>
        /// <param name="picking_department_no">部门</param>
        /// <param name="recommended_operations">建议执行作业</param>
        /// <param name="recommended_function">A.新增  S.过帐</param>
        /// <param name="scan_doc_no">扫描单号</param>
        /// <param name="collScan"></param>
        public DependencyObjectCollection InertTransactionDoc(string employee_no, string scan_type, DateTime report_datetime,
            string picking_department_no, string recommended_operations, string recommended_function,
            string scan_doc_no, DependencyObjectCollection scanColl) {
            IInfoEncodeContainer InfoEncodeSrv = this.GetService<IInfoEncodeContainer>();
            if (Maths.IsEmpty(recommended_operations)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "recommended_operations"));//‘入参【recommended_operations】未传值’
            }
            if (Maths.IsEmpty(recommended_function)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "recommended_function"));//‘入参【recommended_function】未传值’
            }
            DependencyObjectType type = new DependencyObjectType("ReturnCollection");
            type.RegisterSimpleProperty("doc_no", typeof(string));
            DependencyObjectCollection Rtn = new DependencyObjectCollection(type);

            if (scanColl.Count > 0) {
                string stockAction = string.Empty;
                string view = string.Empty;
                if (recommended_operations.StartsWith("11")) {
                    stockAction = "-1";
                    view = "TRANSACTION_DOC.I02";
                } else if (recommended_operations.StartsWith("12")) {
                    stockAction = "1";
                    view = "TRANSACTION_DOC.I01";
                }
                DependencyObjectCollection docColl = ValidateDocSet(stockAction, scanColl[0]["site_no"].ToStringExtension());
                if (docColl.Count <= 0) {
                    throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111275"));
                }
                DataTable dtScan = CreateDtScan();
                DataTable dtScanDetail = CreateDtScanDetail();
                PrepareDtForInsertScan(employee_no, picking_department_no, scanColl, dtScan, dtScanDetail);
                using (IConnectionService connService = this.GetService<IConnectionService>()) {
                    _qurService = this.GetService<IQueryService>();
                    CreateTemp();
                    InsertTemp(dtScan, dtScanDetail);
                    List<DependencyObject> saveEntities = AddEntity(docColl[0]["DOC_ID"], docColl[0]["SEQUENCE_DIGIT"].ToInt32(), report_datetime, stockAction);
                    if (saveEntities.Count > 0) {
                        using (ITransactionService transService = this.GetService<ITransactionService>()) {
                            InsertBCLine(report_datetime);//20170413 add by wangyq for P001-170412001 需要在保存服务之前,保存自动审核会需要回写
                            //保存单据
                            ISaveService saveSrv = this.GetService<ISaveService>("TRANSACTION_DOC");
                            saveSrv.Save(saveEntities.ToArray());

                            UpdateTD();//更新

                            //InsertBCLine(report_datetime);//20170413 mark by wangyq for P001-170412001

                            //EFNET签核
                            foreach (DependencyObject item in saveEntities) {
                                IEFNETStatusStatusService efnetSrv = this.GetService<IEFNETStatusStatusService>();
                                efnetSrv.GetFormFlow(view, item["DOC_ID"], ((DependencyObject)item["Owner_Org"])["ROid"], new object[] { item["TRANSACTION_DOC_ID"] });
                            }
                            transService.Complete();
                        }
                        foreach (DependencyObject item in saveEntities) {
                            DependencyObject obj = Rtn.AddNew();
                            obj["doc_no"] = item["DOC_NO"];
                        }
                    }
                }
            }
            return Rtn;
        }
        /// <summary>
        /// //7.1定义单据类型
        /// </summary>
        /// <param name="recommended_operations"></param>
        /// <param name="siteNo"></param>
        private DependencyObjectCollection ValidateDocSet(string stockAction, string siteNo) {
            QueryConditionGroup group = OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo) &
                OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants("11");
            if (!string.IsNullOrEmpty(stockAction)) {
                group &= OOQL.CreateProperty("PARA_DOC_FIL.STOCK_ACTION") == OOQL.CreateConstants(stockAction);
            }
            QueryNode node = OOQL.Select(OOQL.CreateProperty("DOC.DOC_ID"), OOQL.CreateProperty("DOC.SEQUENCE_DIGIT"))
                 .From("PARA_DOC_FIL", "PARA_DOC_FIL")
                 .InnerJoin("PLANT", "PLANT")
                 .On(OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                 .InnerJoin("DOC", "DOC")
                 .On(OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID") == OOQL.CreateProperty("DOC.DOC_ID"))
                 .Where(OOQL.AuthFilter("PARA_DOC_FIL", "PARA_DOC_FIL") & (group));
            return this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }
        /// <summary>
        /// 为临时表新增组织数据
        /// </summary>
        /// <param name="scanColl"></param>
        /// <param name="dtScan"></param>
        /// <param name="dtScanDetail"></param>
        private void PrepareDtForInsertScan(string employee_no, string picking_department_no, DependencyObjectCollection scanColl, DataTable dtScan, DataTable dtScanDetail) {
            foreach (DependencyObject scanObj in scanColl) {
                DataRow drScan = dtScan.NewRow();
                drScan["employee_no"] = employee_no;
                drScan["picking_department_no"] = picking_department_no; ;
                drScan["site_no"] = scanObj["site_no"];
                drScan["info_lot_no"] = scanObj["info_lot_no"];
                drScan["ID"] = Guid.NewGuid();
                dtScan.Rows.Add(drScan);
                #region  明细
                DependencyObjectCollection scanDetail = scanObj["scan_detail"] as DependencyObjectCollection;
                List<IGrouping<string, DependencyObject>> groupDColl = scanDetail.GroupBy(c => string.Concat(
                                   c["item_no"], c["item_feature_no"], c["picking_unit_no"], c["warehouse_no"], c["storage_spaces_no"], c["lot_no"], c["barcode_no"])).ToList();
                Dictionary<string, DetailEntity> idList = new Dictionary<string, DetailEntity>();
                int seqNumber = 1;
                foreach (IGrouping<string, DependencyObject> groupDObj in groupDColl) {
                    DependencyObject infoObj = groupDObj.ToList()[0];
                    DataRow drDetail = dtScanDetail.NewRow();
                    foreach (DataColumn dcObj in dtScanDetail.Columns) {
                        if (dcObj.ColumnName != "ID" && dcObj.ColumnName != "SequenceNumber") {
                            drDetail[dcObj.ColumnName] = infoObj[dcObj.ColumnName];
                        }
                    }
                    decimal sumQty = groupDObj.Sum(c => c["picking_qty"].ToDecimal());
                    drDetail["picking_qty"] = sumQty;
                    string keyString = string.Concat(infoObj["item_no"], infoObj["item_feature_no"], infoObj["picking_unit_no"],
                        infoObj["warehouse_no"], infoObj["storage_spaces_no"], infoObj["lot_no"]);
                    if (idList.Keys.Contains(keyString)) {
                        drDetail["ID"] = idList[keyString].Id;
                        drDetail["SequenceNumber"] = idList[keyString].SeqNumber;
                    } else {
                        drDetail["ID"] = Guid.NewGuid();
                        drDetail["SequenceNumber"] = seqNumber;
                        idList.Add(keyString, new DetailEntity(drDetail["ID"], seqNumber));
                        seqNumber++;
                    }
                    dtScanDetail.Rows.Add(drDetail);
                }
                #endregion
            }
        }
        /// <summary>
        /// 单头临时表新增的数据源表结构
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDtScan() {
            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("ID", typeof(object)));
            dt.Columns.Add(new DataColumn("employee_no", typeof(string)));
            dt.Columns.Add(new DataColumn("picking_department_no", typeof(string)));
            dt.Columns.Add(new DataColumn("site_no", typeof(string)));
            dt.Columns.Add(new DataColumn("info_lot_no", typeof(string)));
            return dt;
        }
        /// <summary>
        /// 单身临时表新增的数据源表结构
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDtScanDetail() {
            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("ID", typeof(object)));
            dt.Columns.Add(new DataColumn("info_lot_no", typeof(string)));
            dt.Columns.Add(new DataColumn("SequenceNumber", typeof(Int32)));
            dt.Columns.Add(new DataColumn("item_no", typeof(string)));
            dt.Columns.Add(new DataColumn("item_feature_no", typeof(string)));
            dt.Columns.Add(new DataColumn("picking_unit_no", typeof(string)));
            dt.Columns.Add(new DataColumn("warehouse_no", typeof(string)));
            dt.Columns.Add(new DataColumn("storage_spaces_no", typeof(string)));
            dt.Columns.Add(new DataColumn("lot_no", typeof(string)));
            dt.Columns.Add(new DataColumn("picking_qty", typeof(decimal)));
            dt.Columns.Add(new DataColumn("barcode_no", typeof(string)));
            return dt;
        }
        /// <summary>
        /// 创建存储传入参数集合的临时表
        /// </summary>
        private void CreateTemp() {
            IBusinessTypeService businessSrv = this.GetServiceForThisTypeKey<IBusinessTypeService>();

            string typeName = "Temp_Scan" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });

            #region 单头
            defaultType.RegisterSimpleProperty("ID", businessSrv.SimplePrimaryKeyType, Maths.GuidDefaultValue(), false, new Attribute[] { businessSrv.SimplePrimaryKey });
            //人员
            defaultType.RegisterSimpleProperty("employee_no", businessSrv.GetBusinessType("BusinessCode"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("BusinessCode") });
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            tempAttr.Size = 10;
            //部门
            defaultType.RegisterSimpleProperty("picking_department_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //工厂编号
            defaultType.RegisterSimpleProperty("site_no", businessSrv.GetBusinessType("Factory"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("Factory") });
            //信息批号
            tempAttr.Size = 30;
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            _tempScan = defaultType;
            _qurService.CreateTempTable(_tempScan);
            #endregion

            typeName = "Temp_ScanDetail" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            defaultType = new DependencyObjectType(typeName, new Attribute[] { });
            #region 单身
            defaultType.RegisterSimpleProperty("ID", businessSrv.SimplePrimaryKeyType, Maths.GuidDefaultValue(), false, new Attribute[] { businessSrv.SimplePrimaryKey });
            //信息批号
            tempAttr.Size = 30;
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //序号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("SequenceNumber", typeof(Int32), 0, false, new Attribute[] { tempAttr });
            //品号
            defaultType.RegisterSimpleProperty("item_no", businessSrv.GetBusinessType("ItemCode"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("ItemCode") });
            //特征码
            defaultType.RegisterSimpleProperty("item_feature_no", businessSrv.GetBusinessType("ItemFeature"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("ItemFeature") });
            //单位
            defaultType.RegisterSimpleProperty("picking_unit_no", businessSrv.GetBusinessType("UnitCode"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("UnitCode") });
            //仓库
            defaultType.RegisterSimpleProperty("warehouse_no", businessSrv.GetBusinessType("WarehouseCode"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("WarehouseCode") });
            //库位
            defaultType.RegisterSimpleProperty("storage_spaces_no", businessSrv.GetBusinessType("Bin"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("Bin") });
            //批号
            defaultType.RegisterSimpleProperty("lot_no", businessSrv.GetBusinessType("LotCode"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("LotCode") });
            //拣货数量
            defaultType.RegisterSimpleProperty("picking_qty", businessSrv.GetBusinessType("Quantity"), 0M, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("Quantity") });
            //条码编号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            tempAttr.Size = 1000;
            defaultType.RegisterSimpleProperty("barcode_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            _tempScanDetail = defaultType;
            _qurService.CreateTempTable(_tempScanDetail);
            #endregion
        }
        /// <summary>
        /// 临时表新增逻辑
        /// </summary>
        /// <param name="dtScan"></param>
        /// <param name="dtScanDetail"></param>
        private void InsertTemp(DataTable dtScan, DataTable dtScanDetail) {
            List<BulkCopyColumnMapping> mappingList = new List<BulkCopyColumnMapping>();
            foreach (DataColumn dcScan in dtScan.Columns) {
                mappingList.Add(new BulkCopyColumnMapping(dcScan.ColumnName, dcScan.ColumnName));
            }
            _qurService.BulkCopy(dtScan, _tempScan.Name, mappingList.ToArray());

            mappingList.Clear();
            foreach (DataColumn dcDetail in dtScanDetail.Columns) {
                mappingList.Add(new BulkCopyColumnMapping(dcDetail.ColumnName, dcDetail.ColumnName));
            }
            _qurService.BulkCopy(dtScanDetail, _tempScanDetail.Name, mappingList.ToArray());
        }
        /// <summary>
        /// 新增单头查询逻辑
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection QueryForTD() {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("PLANT.PLANT_ID"),
                                      OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"),
                                      OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"),
                                      OOQL.CreateProperty("Table_scan.ID"),
                                      OOQL.CreateProperty("PLANT.COMPANY_ID"),
                                      OOQL.CreateProperty("PLANT.PLANT_ID"),
                                      OOQL.CreateProperty("Table_scan.info_lot_no")
                                      )
                .From(_tempScan.Name, "Table_scan")
                .InnerJoin("PLANT", "PLANT")
                .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .InnerJoin("EMPLOYEE", "EMPLOYEE")
                .On(OOQL.CreateProperty("Table_scan.employee_no") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE"))
                .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                .On(OOQL.CreateProperty("Table_scan.picking_department_no") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE"));
            return _qurService.ExecuteDependencyObject(node);
        }
        /// <summary>
        /// 新增单身查询逻辑
        /// </summary>
        /// <param name="docId"></param>
        /// <returns></returns>
        private DependencyObjectCollection QueryForTD_D(object docId) {
            QueryNode groupNode = GroupNode(false);
            QueryNode node = OOQL.Select(OOQL.CreateProperty("Table_scan_detail.SequenceNumber", "SequenceNumber"),
                                OOQL.CreateProperty("Table_scan_detail.ID", "DETAIL_ID"),
                                OOQL.CreateProperty("Table_scan.ID", "ID"),
                                OOQL.CreateProperty("Table_scan.info_lot_no"),
                                OOQL.CreateProperty("ITEM.ITEM_ID", "ITEM_ID"),
                                OOQL.CreateProperty("ITEM.ITEM_NAME", "ITEM_NAME"),
                                Formulas.Case(null, OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                                              OOQL.CreateCaseArray(
                                                  OOQL.CreateCaseItem(
                                                      OOQL.CreateProperty("Table_scan_detail.item_feature_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String),
                                                      OOQL.CreateConstants(Maths.GuidDefaultValue()))), "ITEM_FEATURE_ID"),
                                OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "ITEM_SPECIFICATION"),
                                OOQL.CreateProperty("Table_scan_detail.picking_qty", "picking_qty"),
                                OOQL.CreateProperty("UNIT.UNIT_ID", "UNIT_ID"),
                                Formulas.Ext("UNIT_CONVERT", "SECOND_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                                OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                                OOQL.CreateProperty("Table_scan_detail.picking_qty"),
                                                                OOQL.CreateProperty("ITEM.SECOND_UNIT_ID"),
                                                                OOQL.CreateConstants (0)
                                                                }),
                                Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                                OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                                OOQL.CreateProperty("Table_scan_detail.picking_qty"),
                                                                OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                                OOQL.CreateConstants (0)
                                                                }),
                                OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID", "WAREHOUSE_ID"),
                                Formulas.Case(null, OOQL.CreateProperty("BIN.BIN_ID"),
                                              OOQL.CreateCaseArray(
                                                  OOQL.CreateCaseItem(
                                                      OOQL.CreateProperty("Table_scan_detail.storage_spaces_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String),
                                                      OOQL.CreateConstants(Maths.GuidDefaultValue()))), "BIN_ID"),
                                Formulas.Case(null, OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"),
                                              OOQL.CreateCaseArray(
                                                  OOQL.CreateCaseItem(
                                                      OOQL.CreateProperty("Table_scan_detail.lot_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String),
                                                      OOQL.CreateConstants(Maths.GuidDefaultValue()))), "ITEM_LOT_ID"),
                                Formulas.Case(null, OOQL.CreateConstants("1", GeneralDBType.String),
                                              OOQL.CreateCaseArray(
                                                  OOQL.CreateCaseItem(
                                                      OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT") == OOQL.CreateConstants(false, GeneralDBType.Boolean)
                                                      | OOQL.CreateProperty("Table_scan_detail.picking_qty") == OOQL.CreateConstants(0M, GeneralDBType.Decimal),
                                                     OOQL.CreateConstants("0", GeneralDBType.String))), "SN_COLLECTED_STATUS"),
                                 OOQL.CreateProperty("PLANT.COMPANY_ID"),
                                 OOQL.CreateProperty("PLANT.PLANT_ID"),
                                 OOQL.CreateProperty("PLANT.COST_DOMAIN_ID", "P_COST_DOMAIN_ID"),
                                 OOQL.CreateProperty("WAREHOUSE.COST_DOMAIN_ID", "W_COST_DOMAIN_ID"),
                                 OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")
                                                      )
                        .From(groupNode, "Table_scan_detail")
                        .InnerJoin(_tempScan.Name, "Table_scan")
                        .On(OOQL.CreateProperty("Table_scan.info_lot_no") == OOQL.CreateProperty("Table_scan_detail.info_lot_no"))
                        .InnerJoin("PLANT", "PLANT")
                        .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                        .InnerJoin("ITEM", "ITEM")
                        .On(OOQL.CreateProperty("Table_scan_detail.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                        .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                        .On(OOQL.CreateProperty("Table_scan_detail.item_feature_no") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE")
                             & OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                        .InnerJoin("UNIT", "UNIT")
                        .On(OOQL.CreateProperty("Table_scan_detail.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                        .LeftJoin("WAREHOUSE", "WAREHOUSE")
                        .On(OOQL.CreateProperty("Table_scan_detail.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                             & OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                        .LeftJoin("WAREHOUSE.BIN", "BIN")
                        .On(OOQL.CreateProperty("Table_scan_detail.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE")
                             & OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                        .LeftJoin("ITEM_LOT", "ITEM_LOT")
                        .On(OOQL.CreateProperty("Table_scan_detail.lot_no") == OOQL.CreateProperty("ITEM_LOT.LOT_CODE")
                             & OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")
                             & ((OOQL.CreateProperty("Table_scan_detail.item_feature_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String)
                                 & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                                | OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID")))
                        .InnerJoin("DOC", "DOC")
                        .On(OOQL.CreateConstants(docId) == OOQL.CreateProperty("DOC.DOC_ID"))
                        .LeftJoin("PARA_COMPANY", "PARA_COMPANY")
                        .On(OOQL.CreateProperty("PLANT.COMPANY_ID") == OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid"));

            return _qurService.ExecuteDependencyObject(node);

        }
        /// <summary>
        /// 新增单头
        /// </summary>
        /// <param name="docId"></param>
        /// <param name="sequenceDigit"></param>
        /// <param name="report_datetime"></param>
        /// <param name="stockAction"></param>
        private List<DependencyObject> AddEntity(object docId, int sequenceDigit, DateTime report_datetime, string stockAction) {
            ICreateService createSrv = GetService<ICreateService>("TRANSACTION_DOC");
            DependencyObject entity = createSrv.Create() as DependencyObject;
            IDocumentNumberGenerateService DocumentNumberGenSrv = this.GetService<IDocumentNumberGenerateService>("TRANSACTION_DOC");
            DependencyObjectCollection queryScanColl = QueryForTD();
            DependencyObjectCollection queryScanDetailColl = QueryForTD_D(docId);

            string docNo = string.Empty;
            List<DependencyObject> newAddList = new List<DependencyObject>();
            foreach (DependencyObject tdObj in queryScanColl) {
                DependencyObject newTD = new DependencyObject(entity.DependencyObjectType);
                newTD["DOC_ID"] = docId;
                newTD["DOC_DATE"] = report_datetime.Date;
                if (string.IsNullOrEmpty(docNo)) {
                    docNo = UtilsClass.NextNumber(DocumentNumberGenSrv, "", docId, sequenceDigit, report_datetime.Date);
                } else {
                    docNo = UtilsClass.NextNumber(DocumentNumberGenSrv, docNo, docId, sequenceDigit, report_datetime.Date);
                }
                newTD["DOC_NO"] = docNo;
                newTD["TRANSACTION_DATE"] = report_datetime.Date;
                newTD["CATEGORY"] = "11";
                newTD["STOCK_ACTION"] = stockAction;
                ((DependencyObject)newTD["Owner_Org"])["RTK"] = "PLANT";
                ((DependencyObject)newTD["Owner_Org"])["ROid"] = tdObj["PLANT_ID"];
                newTD["Owner_Dept"] = tdObj["ADMIN_UNIT_ID"];
                newTD["Owner_Emp"] = tdObj["EMPLOYEE_ID"];
                newTD["TRANSACTION_DOC_ID"] = tdObj["ID"];
                newTD["COMPANY_ID"] = tdObj["COMPANY_ID"];

                DependencyObjectCollection entityDColl = newTD["TRANSACTION_DOC_D"] as DependencyObjectCollection;
                List<DependencyObject> detailScanList = queryScanDetailColl.Where(c => c["info_lot_no"].ToStringExtension() == tdObj["info_lot_no"].ToStringExtension()).ToList();
                if (detailScanList.Count > 0) {
                    AddEntity_D(entityDColl, detailScanList);
                }
                newAddList.Add(newTD);
            }
            return newAddList;
        }
        /// <summary>
        /// 新增单身
        /// </summary>
        /// <param name="entityDColl"></param>
        /// <param name="detailScanList"></param>
        private void AddEntity_D(DependencyObjectCollection entityDColl, List<DependencyObject> detailScanList) {
            foreach (DependencyObject detailObj in detailScanList) {
                DependencyObject addDetail = entityDColl.AddNew();
                addDetail["SequenceNumber"] = detailObj["SequenceNumber"];
                addDetail["TRANSACTION_DOC_D_ID"] = detailObj["DETAIL_ID"];
                addDetail["ITEM_ID"] = detailObj["ITEM_ID"];
                addDetail["ITEM_DESCRIPTION"] = detailObj["ITEM_NAME"];
                addDetail["ITEM_FEATURE_ID"] = detailObj["ITEM_FEATURE_ID"];
                addDetail["ITEM_SPECIFICATION"] = detailObj["ITEM_SPECIFICATION"];
                ((DependencyObject)addDetail["BO_ID"])["RTK"] = "OTHER";
                ((DependencyObject)addDetail["BO_ID"])["ROid"] = Maths.GuidDefaultValue();
                addDetail["BUSINESS_QTY"] = detailObj["picking_qty"];
                addDetail["BUSINESS_UNIT_ID"] = detailObj["UNIT_ID"];
                addDetail["SECOND_QTY"] = detailObj["SECOND_QTY"];
                addDetail["INVENTORY_QTY"] = detailObj["INVENTORY_QTY"];
                addDetail["WAREHOUSE_ID"] = detailObj["WAREHOUSE_ID"];
                addDetail["BIN_ID"] = detailObj["BIN_ID"];
                addDetail["ITEM_LOT_ID"] = detailObj["ITEM_LOT_ID"];
                ((DependencyObject)addDetail["SOURCE_ID"])["RTK"] = "OTHER";
                ((DependencyObject)addDetail["SOURCE_ID"])["ROid"] = Maths.GuidDefaultValue();
                ((DependencyObject)addDetail["REFERENCE_SOURCE_ID"])["RTK"] = "OTHER";
                ((DependencyObject)addDetail["REFERENCE_SOURCE_ID"])["ROid"] = Maths.GuidDefaultValue();
                string invLevel = detailObj["INVENTORY_VALUATION_LEVEL"].ToStringExtension();
                if (invLevel == "1") {
                    ((DependencyObject)addDetail["COST_DOMAIN_ID"])["RTK"] = "COMPANY";
                    ((DependencyObject)addDetail["COST_DOMAIN_ID"])["ROid"] = detailObj["COMPANY_ID"];
                } else {
                    ((DependencyObject)addDetail["COST_DOMAIN_ID"])["RTK"] = "COST_DOMAIN";
                    if (invLevel == "2") {
                        ((DependencyObject)addDetail["COST_DOMAIN_ID"])["ROid"] = detailObj["P_COST_DOMAIN_ID"];
                    } else if (invLevel == "3") {
                        ((DependencyObject)addDetail["COST_DOMAIN_ID"])["ROid"] = detailObj["W_COST_DOMAIN_ID"];
                    }
                }
                addDetail["REMARK"] = string.Empty;
                addDetail["ApproveStatus"] = "N";
                addDetail["ApproveDate"] = OrmDataOption.EmptyDateTime;
                addDetail["SN_COLLECTED_STATUS"] = detailObj["SN_COLLECTED_STATUS"];
                addDetail["PLANT_ID"] = detailObj["PLANT_ID"];
            }
        }
        /// <summary>
        /// //7.2.3更新单头业务数量汇总
        /// </summary>
        private void UpdateTD() {
            QueryNode selectNode = OOQL.Select(OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID"),
                Formulas.Sum("BUSINESS_QTY", "SUM_BUSINESS_QTY"))
                .From("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                .InnerJoin(_tempScan.Name, "temp")
                .On(OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID") == OOQL.CreateProperty("temp.ID"))
                .GroupBy(OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID"));
            QueryNode node = OOQL.Update("TRANSACTION_DOC")
                .Set(new SetItem[] { new SetItem(OOQL.CreateProperty("SUM_BUSINESS_QTY"), OOQL.CreateProperty("selectNode.SUM_BUSINESS_QTY")) })
                .From(selectNode, "selectNode")
                .Where(OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID") == OOQL.CreateProperty("selectNode.TRANSACTION_DOC_ID"));
            _qurService.ExecuteNoQueryWithManageProperties(node);
        }
        /// <summary>
        /// 利用临时表关联实体表进行批量新增条码交易明细
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpBCLine"></param>
        private void InsertBCLine(DateTime report_datetime) {//20170413 modi by wangyq for P001-170412001 添加参数DateTime report_datetime
            bool bcLintFlag = UtilsClass.IsBCLineManagement(_qurService);
            if (!bcLintFlag)
                return;
            List<QueryProperty> selectList = new List<QueryProperty>();
            #region 查询新增字段集合
            selectList.Add(Formulas.NewId("BC_LINE_ID"));
            selectList.Add(OOQL.CreateProperty("tmpTable.barcode_no", "BARCODE_NO"));
            selectList.Add(OOQL.CreateConstants("TRANSACTION_DOC.TRANSACTION_DOC_D", "SOURCE_ID_RTK"));
            selectList.Add(OOQL.CreateProperty("tmpTable.ID", "SOURCE_ID_ROid"));
            selectList.Add(Formulas.Ext("UNIT_CONVERT", "QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}));
            selectList.Add(Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"));
            selectList.Add(Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("BIN.BIN_ID"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.storage_spaces_no")==OOQL.CreateConstants(string.Empty)
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID"));
            //20170413 modi by wangyq for P001-170412001   ==================begin=================
            //20170330 add by wangrm for P001-170328001=====start=======   
            selectList.Add(OOQL.CreateConstants("PLANT", GeneralDBType.String, "Owner_Org_RTK"));
            selectList.Add(OOQL.CreateProperty("PLANT.PLANT_ID", "Owner_Org_ROid"));
            selectList.Add(OOQL.CreateProperty("Table_scan.ID", "SOURCE_DOC_ID"));
            selectList.Add(OOQL.CreateConstants(report_datetime.Date, GeneralDBType.Date, "DOC_DATE"));
            //selectList.Add(Formulas.IsNull(OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.RTK"), OOQL.CreateConstants(string.Empty), "Owner_Org_RTK"));
            //selectList.Add(Formulas.IsNull(OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.ROid"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org_ROid"));
            //selectList.Add(Formulas.IsNull(OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_DOC_ID"));
            //selectList.Add(Formulas.IsNull(OOQL.CreateProperty("TRANSACTION_DOC.DOC_DATE"), OOQL.CreateConstants(OrmDataOption.EmptyDateTime), "DOC_DATE"));
            //20170330 add by wangrm for P001-170328001=====end=======
            //20170413 modi by wangyq for P001-170412001   ==================end=================

            #endregion

            QueryNode groupNode = GroupNode(true);
            QueryNode insertNode = OOQL.Select(selectList.ToArray())
                .From(groupNode, "tmpTable")
                .InnerJoin(_tempScan.Name, "Table_scan")
                .On(OOQL.CreateProperty("Table_scan.info_lot_no") == OOQL.CreateProperty("tmpTable.info_lot_no"))
                .InnerJoin("PLANT", "PLANT")
                .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .InnerJoin("ITEM", "ITEM")
                .On(OOQL.CreateProperty("tmpTable.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .InnerJoin("UNIT", "UNIT")
                .On(OOQL.CreateProperty("tmpTable.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                .LeftJoin("WAREHOUSE", "WAREHOUSE")
                .On(OOQL.CreateProperty("tmpTable.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                    & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("tmpTable.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE"))
                //20170413 mark by wangyq for P001-170412001   ==================begin================
                //20170330 add by wangrm for P001-170328001=====start=======
                //.LeftJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
                //.On(OOQL.CreateProperty("Table_scan.ID") == OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID"))
                //20170330 add by wangrm for P001-170328001=====end=======
                //20170413 mark by wangyq for P001-170412001   ==================end================
                .Where(OOQL.CreateProperty("tmpTable.barcode_no") != OOQL.CreateConstants(string.Empty));

            List<string> insertStrList = new List<string>();
            #region 新增字段集合
            insertStrList.Add("BC_LINE_ID");
            insertStrList.Add("BARCODE_NO");
            insertStrList.Add("SOURCE_ID.RTK");
            insertStrList.Add("SOURCE_ID.ROid");
            insertStrList.Add("QTY");
            insertStrList.Add("WAREHOUSE_ID");
            insertStrList.Add("BIN_ID");
            //20170330 add by wangrm for P001-170328001=====start=======
            insertStrList.Add("Owner_Org_RTK");
            insertStrList.Add("Owner_Org_ROid");
            insertStrList.Add("SOURCE_DOC_ID");
            insertStrList.Add("DOC_DATE");
            //20170330 add by wangrm for P001-170328001=====end=======
            #endregion
            QueryNode node = OOQL.Insert("BC_LINE", insertNode, insertStrList.ToArray());
            _qurService.ExecuteNoQueryWithManageProperties(node);
        }
        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="tmpIssueReceiptD"></param>
        /// <returns></returns>
        public QueryNode GroupNode(bool isEntityLine) {
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.Add(OOQL.CreateProperty("TMP.ID"));
            properties.Add(OOQL.CreateProperty("TMP.SequenceNumber"));
            properties.Add(OOQL.CreateProperty("TMP.item_no"));
            properties.Add(OOQL.CreateProperty("TMP.item_feature_no"));
            properties.Add(OOQL.CreateProperty("TMP.picking_unit_no"));
            properties.Add(OOQL.CreateProperty("TMP.warehouse_no"));
            properties.Add(OOQL.CreateProperty("TMP.storage_spaces_no"));
            properties.Add(OOQL.CreateProperty("TMP.lot_no"));
            properties.Add(OOQL.CreateProperty("TMP.info_lot_no"));
            if (isEntityLine) {
                properties.Add(OOQL.CreateProperty("TMP.barcode_no"));
            }

            List<QueryProperty> groupProperties = new List<QueryProperty>();
            groupProperties = new List<QueryProperty>();
            groupProperties.AddRange(properties);

            properties.Add(Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("TMP.picking_qty")), OOQL.CreateConstants(0), "picking_qty"));

            QueryNode node = OOQL.Select(properties.ToArray())
                                  .From(_tempScanDetail.Name, "TMP")
                                  .GroupBy(groupProperties);

            return node;
        }
    }

    public class DetailEntity {
        public DetailEntity(object id, int seq) {
            Id = id;
            SeqNumber = seq;
        }
        public object Id { get; set; }
        public int SeqNumber { get; set; }
    }
}
